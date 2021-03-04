// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_stm.h"

#include "raft/errc.h"
#include "kafka/protocol/response_writer.h"
#include "cluster/logger.h"
#include "kafka/protocol/request_reader.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"
#include "storage/parser_utils.h"
#include "model/record.h"
#include <seastar/core/coroutine.hh>

#include <seastar/core/future.hh>

#include <filesystem>

namespace cluster {
using namespace std::chrono_literals;

tx_stm::tx_stm(
  ss::logger& logger, raft::consensus* c, [[maybe_unused]] config::configuration& config)
  : persisted_stm("tx", logger, c, config) {}

static iobuf make_control_record_key(model::control_record_type crt) {
    iobuf b;
    kafka::response_writer w(b);
    w.write(model::current_control_record_version());
    w.write((int16_t)crt);
    return b;
}

static iobuf make_prepare_control_record_key(model::partition_id tm) {
    iobuf b;
    kafka::response_writer w(b);
    w.write(tx_stm::prepare_control_record_version());
    w.write(tm);
    return b;
}

static model::record make_control_record(iobuf k) {
    int index = 0;
    static constexpr size_t zero_vint_size = vint::vint_size(0);
    auto k_z = k.size_bytes();
    auto size = sizeof(model::record_attributes::type) // attributes
                + vint::vint_size(0)                   // timestamp delta
                + vint::vint_size(0)                   // offset delta
                + vint::vint_size(k_z)                 // key size
                + k.size_bytes()                       // key payload
                + zero_vint_size                       // value size
                + zero_vint_size;                      // headers size
    return model::record(
      size,
      model::record_attributes(0),
      index,
      index,
      k_z,
      std::move(k),
      0,
      iobuf{},
      std::vector<model::record_header>{});
}

static model::record_batch_header make_control_header(
  model::record_batch_type type,
  model::producer_identity pid) {
    auto ts = model::timestamp::now()();
    return model::record_batch_header{
      .size_bytes = 0, // computed later
      .base_offset = model::offset(0), // ?
      .type = type,
      .crc = 0, // we-reassign later
      .attrs = model::record_batch_attributes(model::record_batch_attributes::control_mask),
      .last_offset_delta = 0,
      .first_timestamp = model::timestamp(ts),
      .max_timestamp = model::timestamp(ts),
      .producer_id = pid.id,
      .producer_epoch = pid.epoch,
      .base_sequence = 0,
      .record_count = 0};
}

static model::record_batch make_control_record_batch(model::producer_identity pid, model::control_record_type crt) {
    auto header = make_control_header(raft::data_batch_type, pid);
    header.record_count = 1;
    auto key = make_control_record_key(crt);
    iobuf records;
    model::append_record_to_buffer(
        records,
        make_control_record(std::move(key)));
    storage::internal::reset_size_checksum_metadata(header, records);
    return model::record_batch(header, std::move(records), model::record_batch::tag_ctor_ng{});
}

static model::record_batch make_prepare_control_record_batch(model::producer_identity pid, model::partition_id tm) {
    auto header = make_control_header(tx_prepare_batch_type, pid);
    header.record_count = 1;
    auto key = make_prepare_control_record_key(tm);
    iobuf records;
    model::append_record_to_buffer(
        records,
        make_control_record(std::move(key)));
    storage::internal::reset_size_checksum_metadata(header, records);
    return model::record_batch(header, std::move(records), model::record_batch::tag_ctor_ng{});
}

static model::record_batch make_fence_control_record_batch(model::producer_identity pid) {
    auto header = make_control_header(tx_fence_batch_type, pid);
    iobuf records;
    storage::internal::reset_size_checksum_metadata(header, records);
    return model::record_batch(header, std::move(records), model::record_batch::tag_ctor_ng{});
}

static inline tx_stm::producer_id id(model::producer_identity pid) {
    return tx_stm::producer_id(pid.id);
}

static inline tx_stm::producer_epoch epoch(model::producer_identity pid) {
    return tx_stm::producer_epoch(pid.epoch);
}

// called stricted after a tx was canceled on the coordinator
// the sole purpose is to put tx_range into the list of aborted txes
// and to fence off the old epoch
ss::future<tx_errc>
tx_stm::abort_tx(model::producer_identity pid, [[maybe_unused]] model::timeout_clock::time_point timeout) {
    // <fencing>
    // doesn't make sense to fence off an abort because a decision to 
    // has already been make on the tx coordinator and there is no way
    // to force it to commit
    // </fencing>

    auto is_ready = co_await is_caught_up(2'000ms);
    if (!is_ready) {
        co_return tx_errc::timeout;
    }

    auto batch = make_control_record_batch(pid, model::control_record_type::tx_abort);

    auto r = co_await _c->replicate(
        _insync_term,
        model::make_memory_record_batch_reader(std::move(batch)),
        raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!r) {
        co_return tx_errc::timeout;
    }

    // don't need to wait for apply because tx is already aborted on the
    // coordinator level - nothing can go wrong
    co_return tx_errc::success;
}

ss::future<tx_errc>
tx_stm::prepare_tx(model::term_id etag, model::partition_id tm, model::producer_identity pid, [[maybe_unused]] model::timeout_clock::time_point timeout) {
    // <fencing>
    auto fence_it = _fence_pid_epoch.find(id(pid));
    if (fence_it != _fence_pid_epoch.end()) {
        if (epoch(pid) < fence_it->second) {
            co_return tx_errc::timeout;
        }
    }
    // </fencing>
    
    // <optimization>
    if (_prepared.contains(pid)) {
        co_return tx_errc::success;
    }
    auto expected_it = _expected.find(id(pid));
    if (expected_it == _expected.end()) {
        co_return tx_errc::conflict;
    }
    if (expected_it->second != etag) {
        co_return tx_errc::conflict;
    }
    // </optimization>

    if (_has_prepare_applied.contains(pid)) {
        co_return tx_errc::conflict;
    }
    _has_prepare_applied.emplace(pid, false);

    auto batch = make_prepare_control_record_batch(pid, tm);
    auto r = co_await _c->replicate(
        etag,
        model::make_memory_record_batch_reader(std::move(batch)),
        raft::replicate_options(raft::consistency_level::quorum_ack));
    
    if (!r) {
        co_return tx_errc::timeout;
    }

    co_await wait(model::offset(r.value().last_offset()), model::no_timeout);

    auto has_applied_it = _has_prepare_applied.find(pid);
    if (has_applied_it == _has_prepare_applied.end()) {
        co_return tx_errc::timeout;
    }
    auto applied = has_applied_it->second;
    _has_prepare_applied.erase(pid);

    if (!applied) {
        co_return tx_errc::conflict;
    }

    co_return tx_errc::success;
}

ss::future<tx_errc>
tx_stm::commit_tx(model::producer_identity pid, [[maybe_unused]] model::timeout_clock::time_point timeout) {
    // <fencing>
    // doesn't make sense to fence off a commit because a decision to 
    // commit has already been make on the tx coordinator and there is
    // no way to force it to undo it because it could already comitted
    // on another partition
    // </fencing>
    
    auto is_ready = co_await is_caught_up(2'000ms);
    if (!is_ready) {
        co_return tx_errc::timeout;
    }

    if (_has_commit_applied.contains(pid)) {
        co_return tx_errc::conflict;
    }
    _has_commit_applied.emplace(pid, false);

    auto batch = make_control_record_batch(pid, model::control_record_type::tx_commit);

    auto r = co_await _c->replicate(
        _insync_term,
        model::make_memory_record_batch_reader(std::move(batch)),
        raft::replicate_options(raft::consistency_level::quorum_ack));
    
    if (!r) {
        co_return tx_errc::timeout;
    }
    co_await wait(model::offset(r.value().last_offset()), model::no_timeout);

    auto has_commit_it = _has_commit_applied.find(pid);
    if (has_commit_it == _has_commit_applied.end()) {
        co_return tx_errc::timeout;
    }
    auto applied = has_commit_it->second;
    _has_commit_applied.erase(pid);

    if (!applied) {
        co_return tx_errc::conflict;
    }
    
    co_return tx_errc::success;
}

ss::future<std::optional<model::term_id>>
tx_stm::begin_tx(model::producer_identity pid) {
    auto is_ready = co_await is_caught_up(2'000ms);
    if (!is_ready) {
        co_return std::nullopt;
    }

    // can't begin a tx if there is another tx with the
    // same id is going on

    // <fencing>
    auto fence_it = _fence_pid_epoch.find(id(pid));
    if (fence_it == _fence_pid_epoch.end() || epoch(pid) > fence_it->second) {
        auto batch = make_fence_control_record_batch(pid);
        auto r = co_await _c->replicate(
            _insync_term,
            model::make_memory_record_batch_reader(std::move(batch)),
            raft::replicate_options(raft::consistency_level::quorum_ack));
        if (!r) {
            co_return std::nullopt;
        }
        co_await wait(model::offset(r.value().last_offset()), model::no_timeout);
        fence_it = _fence_pid_epoch.find(id(pid));
        if (fence_it == _fence_pid_epoch.end()) {
            co_return std::nullopt;
        }
    }
    if (epoch(pid) != fence_it->second) {
        co_return std::nullopt;
    }
    // </fencing>

    // <optimization>
    if (_expected.find(id(pid)) != _expected.end()) {
        co_return std::nullopt;
    }
    _expected.emplace(id(pid), _insync_term);
    // </optimization>

    co_return std::optional<model::term_id>(_insync_term);
}

std::optional<model::offset>
tx_stm::tx_lso() {
    std::optional<model::offset> min = std::nullopt;
    
    if (!_ongoing_set.empty()) {
        min = std::optional<model::offset>(*_ongoing_set.begin());
    }

    for (auto& entry : _estimated) {
        if (!min.has_value()) {
            min = std::optional<model::offset>(entry.second);
        }
        if (entry.second < min.value()) {
            min = std::optional<model::offset>(entry.second);
        }
    }

    if (min.has_value()) {
        return std::optional<model::offset>(min.value()-1);
    }

    return std::nullopt;
}

std::vector<tx_stm::tx_range>
tx_stm::aborted_transactions(model::offset from, model::offset to) {
    std::vector<tx_stm::tx_range> result;
    for (auto& range : _aborted) {
        if (range.last < from) {
            continue;
        }
        if (range.first > to) {
            continue;
        }
        result.push_back(range);
    }
    return result;
}

ss::future<checked<raft::replicate_result, kafka::error_code>>
tx_stm::replicate(
  model::batch_identity bid,
  model::record_batch_reader&& br,
  [[maybe_unused]] raft::replicate_options opts) {
    auto is_ready = co_await is_caught_up(2'000ms);
    if (!is_ready) {
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }
    
    auto term = _expected.find(id(bid.pid));
    if (term == _expected.end()) {
        // TODO: sane error
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }
    
    if (_estimated.contains(bid.pid)) {
        // TODO: sane error
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }

    if (!_ongoing_map.contains(bid.pid) && !_estimated.contains(bid.pid)) {
        _estimated.emplace(bid.pid, _insync_offset);
    }

    auto r = co_await _c->replicate(term->second, std::move(br), raft::replicate_options(raft::consistency_level::leader_ack));
    if (!r) {
        // TODO: check that failed replicate lead to aborted tx
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }

    auto b = r.value();
    auto last_offset = model::offset(b.last_offset());
    auto ongoing_it = _ongoing_map.find(bid.pid);
    if (ongoing_it != _ongoing_map.end()) {
        if (ongoing_it->second.last < last_offset) {
            ongoing_it->second.last = last_offset;
        }
    } else {
        auto base_offset = model::offset(last_offset() - (bid.record_count - 1));
        _ongoing_map.emplace(bid.pid, tx_range {
            .pid = bid.pid,
            .first = base_offset,
            .last = model::offset(last_offset)
        });
        _ongoing_set.insert(base_offset);
        _estimated.erase(bid.pid);
    }
    co_return checked<raft::replicate_result, kafka::error_code>(b);
}

void tx_stm::compact_snapshot() { }

ss::future<> tx_stm::apply(model::record_batch b) {
    auto offset = b.last_offset();
    replay(std::move(b));
    _insync_offset = offset;
    return ss::now();
}

void tx_stm::replay(model::record_batch&& b) {
    const auto& hdr = b.header();
    auto bid = model::batch_identity::from(hdr);
    auto pid = bid.pid;

    if (hdr.type == tx_fence_batch_type) {
        auto fence_it = _fence_pid_epoch.find(id(pid));
        if (fence_it == _fence_pid_epoch.end()) {
            _fence_pid_epoch.emplace(id(pid), epoch(pid));
        } else if (fence_it->second < epoch(pid)) {
            fence_it->second=epoch(pid);
            _expected.erase(id(pid));
        }
        return;
    }
    
    if (hdr.type == tx_prepare_batch_type) {
        auto has_applied_it = _has_prepare_applied.find(pid);
        
        auto fence_it = _fence_pid_epoch.find(id(pid));
        if (fence_it == _fence_pid_epoch.end()) {
            _fence_pid_epoch.emplace(id(pid), epoch(pid));
        } else if (fence_it->second < epoch(pid)) {
            fence_it->second=epoch(pid);
            _expected.erase(id(pid));
        } else if (fence_it->second > epoch(pid)) {
            if (has_applied_it != _has_prepare_applied.end()) {
                has_applied_it->second = false;
            }
            return;
        }
        
        _expected.erase(id(pid));
        _prepared.insert(pid);
        // TODO: check if already aborted
        // TODO: check fencing
        
        if (has_applied_it != _has_prepare_applied.end()) {
            has_applied_it->second = true;
        }
        return;
    }
    
    if (hdr.type == raft::data_batch_type && hdr.attrs.is_control()) {
        vassert(b.record_count() == 1, "control record must contain a single record");
        auto r = b.copy_records();
        auto& record = *r.begin();
        auto key = record.release_key();
        kafka::request_reader key_reader(std::move(key));
        auto version = model::control_record_version(key_reader.read_int16());
        vassert(version == model::current_control_record_version, "unknown control record version");
        
        auto fence_it = _fence_pid_epoch.find(id(pid));
        if (fence_it == _fence_pid_epoch.end()) {
            _fence_pid_epoch.emplace(id(pid), epoch(pid));
        } else if (fence_it->second < epoch(pid)) {
            fence_it->second=epoch(pid);
            _expected.erase(id(pid));
        } else if (fence_it->second > epoch(pid)) {
            return;
        }
        
        auto crt = model::control_record_type(key_reader.read_int16());
        if (crt == model::control_record_type::tx_abort) {
            // TODO: update fencing
            _prepared.erase(pid);
            _expected.erase(id(pid));
            _has_prepare_applied.erase(pid);
            _has_commit_applied.erase(pid);
            auto offset_it = _ongoing_map.find(pid);
            if (offset_it != _ongoing_map.end()) {
                _aborted.push_back(offset_it->second);
                _ongoing_set.erase(offset_it->second.first);
                _ongoing_map.erase(pid);
            }
        }
        
        if (crt == model::control_record_type::tx_commit) {
            auto applied = true;
            if (!_prepared.contains(pid)) {
                applied = false;
            }
            // TODO: check if already aborted
            // TODO: check fencing
            auto has_applied_it = _has_commit_applied.find(pid);
            if (has_applied_it != _has_commit_applied.end()) {
                has_applied_it->second = applied;
            }
            _has_prepare_applied.erase(pid);
            _prepared.erase(pid);
            auto offset_it = _ongoing_map.find(pid);
            if (offset_it != _ongoing_map.end()) {
                _ongoing_set.erase(offset_it->second.first);
                _ongoing_map.erase(pid);
            }
        }
        return;
    }
    
    if (hdr.type == raft::data_batch_type) {
        auto last_offset = model::offset(b.last_offset());
        auto ongoing_it = _ongoing_map.find(bid.pid);
        if (ongoing_it != _ongoing_map.end()) {
            if (ongoing_it->second.last < last_offset) {
                ongoing_it->second.last = last_offset;
            }
        } else {
            auto base_offset = model::offset(last_offset() - (bid.record_count - 1));
            _ongoing_map.emplace(bid.pid, tx_range {
                .pid = bid.pid,
                .first = base_offset,
                .last = model::offset(last_offset)
            });
            _ongoing_set.insert(base_offset);
            _estimated.erase(bid.pid);
        }
    }
}

void tx_stm::load_snapshot(stm_snapshot_header hdr, iobuf&& tx_ss_buf) {
    vassert(hdr.version == supported_version, "unsupported seq_snapshot_header version {}", hdr.version);
    iobuf_parser data_parser(std::move(tx_ss_buf));
    auto data = reflection::adl<tx_snapshot>{}.from(data_parser);

    for (auto& entry : data.ongoing) {
        _ongoing_map.emplace(entry.pid, entry);
        _ongoing_set.insert(entry.first);
    }
    for (auto& entry : data.prepared) {
        _prepared.insert(entry);
    }
    for (auto& entry : data.aborted) {
        _aborted.push_back(entry);
    }
    
    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;
}

stm_snapshot tx_stm::take_snapshot() {
    tx_snapshot tx_ss;
    
    for (auto& entry : _ongoing_map) {
        tx_ss.ongoing.push_back(entry.second);
    }
    for (auto& entry : _prepared) {
        tx_ss.prepared.push_back(entry);
    }
    for (auto& entry : _aborted) {
        tx_ss.aborted.push_back(entry);
    }
    tx_ss.offset = _insync_offset;
    
    iobuf tx_ss_buf;
    reflection::adl<tx_snapshot>{}.to(tx_ss_buf, tx_ss);
    
    stm_snapshot_header header;
    header.version = supported_version;
    header.snapshot_size = tx_ss_buf.size_bytes();

    stm_snapshot stx_ss;
    stx_ss.header = header;
    stx_ss.offset = _insync_offset;
    stx_ss.data = std::move(tx_ss_buf);
    return stx_ss;
}

} // namespace raft
