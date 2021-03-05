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

ss::future<std::optional<model::term_id>>
tx_stm::begin_tx(model::producer_identity pid) {
    auto is_ready = co_await sync(2'000ms);
    if (!is_ready) {
        co_return std::nullopt;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state {
            .term = _insync_term
        };
    }

    // <fencing>
    auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
    if (fence_it == _log_state.fence_pid_epoch.end() || epoch(pid) > fence_it->second) {
        auto batch = make_fence_control_record_batch(pid);
        auto r = co_await _c->replicate(
            _insync_term,
            model::make_memory_record_batch_reader(std::move(batch)),
            raft::replicate_options(raft::consistency_level::quorum_ack));
        if (!r) {
            co_return std::nullopt;
        }
        co_await wait(model::offset(r.value().last_offset()), model::no_timeout);
        fence_it = _log_state.fence_pid_epoch.find(id(pid));
        if (fence_it == _log_state.fence_pid_epoch.end()) {
            co_return std::nullopt;
        }
    }
    if (epoch(pid) != fence_it->second) {
        // while we were another fence has passed and cut us off
        // e.g. abort
        co_return std::nullopt;
    }
    // </fencing>

    // <check>
    // if this pid is in use, if check passes it's a violation
    // tx coordinator should begin only after re(commit) or re(abort)
    // todo: add logging or vassert or wipe all tx state depending on
    //       the config
    if (_mem_state.expected.contains(pid)) {
        co_return std::nullopt;
    }
    _mem_state.expected.insert(pid);
    // </check>

    co_return std::optional<model::term_id>(_mem_state.term);
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

    auto is_ready = co_await sync(2'000ms);
    if (!is_ready) {
        co_return tx_errc::timeout;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state {
            .term = _insync_term
        };
    }

    // preventing prepare and replicte once we
    // know we're going to abort the tx
    _mem_state.expected.erase(pid);

    auto batch = make_control_record_batch(pid, model::control_record_type::tx_abort);

    //  once abort in replay all ongoing txes became historic & replay will:
    //   * set fencing
    //   * abort historic ongoing txes with same p.id
    //   * or do nothing if it's fenced out
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
    auto is_ready = co_await sync(2'000ms);
    if (!is_ready) {
        co_return tx_errc::timeout;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state {
            .term = _insync_term
        };
    }

    if (_log_state.prepared.contains(pid)) {
        co_return tx_errc::success;
    }

    // <fencing>
    auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
    if (fence_it != _log_state.fence_pid_epoch.end()) {
        if (epoch(pid) < fence_it->second) {
            co_return tx_errc::timeout;
        }
    }
    // </fencing>

    if (_mem_state.term != etag) {
        co_return tx_errc::timeout;
    }

    // <check>
    if (!_mem_state.expected.contains(pid)) {
        co_return tx_errc::timeout;
    }
    // </check>

    if (_mem_state.has_prepare_applied.contains(pid)) {
        // already tried to prepare and failed
        // or there is a concurrent prepare
        // todo: logging
        co_return tx_errc::timeout;
    }
    _mem_state.has_prepare_applied.emplace(pid, false);

    auto batch = make_prepare_control_record_batch(pid, tm);
    auto r = co_await _c->replicate(
        etag,
        model::make_memory_record_batch_reader(std::move(batch)),
        raft::replicate_options(raft::consistency_level::quorum_ack));
    
    if (!r) {
        co_return tx_errc::timeout;
    }

    co_await wait(model::offset(r.value().last_offset()), model::no_timeout);

    auto has_applied_it = _mem_state.has_prepare_applied.find(pid);
    if (has_applied_it == _mem_state.has_prepare_applied.end()) {
        co_return tx_errc::timeout;
    }
    auto applied = has_applied_it->second;
    _mem_state.has_prepare_applied.erase(pid);

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
    
    auto is_ready = co_await sync(2'000ms);
    if (!is_ready) {
        co_return tx_errc::timeout;
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state {
            .term = _insync_term
        };
    }

    // commit prepared => ok
    // commit empty => ok
    // commit aborted => error (log, vassert, ignore)
    // commit ongoing => error (log, vassert, ignore)

    auto batch = make_control_record_batch(pid, model::control_record_type::tx_commit);

    auto r = co_await _c->replicate(
        _insync_term,
        model::make_memory_record_batch_reader(std::move(batch)),
        raft::replicate_options(raft::consistency_level::quorum_ack));
    
    if (!r) {
        co_return tx_errc::timeout;
    }
    co_await wait(model::offset(r.value().last_offset()), model::no_timeout);

    co_return tx_errc::success;
}

ss::future<checked<raft::replicate_result, kafka::error_code>>
tx_stm::replicate(
  [[maybe_unused]] model::batch_identity bid,
  model::record_batch_reader&& br,
  [[maybe_unused]] raft::replicate_options opts) {
    
    /*return sync(2'000ms).then([this](auto is_ready) {
        if (!is_ready) {
            return ss::make_ready_future<checked<raft::replicate_result, kafka::error_code>>(checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error));
        }
        if (_mem_state.term != _insync_term) {
            _mem_state = mem_state {
                .term = _insync_term
            };
        }
        return _c->replicate(_mem_state.term, std::move(br), raft::replicate_options(raft::consistency_level::leader_ack)).then([](auto r) {
            if (!r) {
                return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
            }
            auto b = r.value();
            return checked<raft::replicate_result, kafka::error_code>(b);
        });
    });*/

    ///////////////////////////////////////////
    
    vlog(clusterlog.info, "SHAI: produce/replicate");
    
    auto is_ready = co_await sync(2'000ms);
    if (!is_ready) {
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state {
            .term = _insync_term
        };
    }

    vlog(clusterlog.info, "SHAI: replicating");
    auto r = co_await _c->replicate(_mem_state.term, std::move(br), raft::replicate_options(raft::consistency_level::leader_ack));
    if (!r) {
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }
    auto b = r.value();
    co_return checked<raft::replicate_result, kafka::error_code>(b);


    //////////////////////////////////////////////////////////


    /*auto is_ready = co_await sync(2'000ms);
    if (!is_ready) {
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }
    if (_mem_state.term != _insync_term) {
        _mem_state = mem_state {
            .term = _insync_term
        };
    }

    vlog(clusterlog.info, "SHAI: synced");
    
    // <fencing>
    auto fence_it = _log_state.fence_pid_epoch.find(id(bid.pid));
    if (fence_it != _log_state.fence_pid_epoch.end()) {
        if (epoch(bid.pid) < fence_it->second) {
            co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
        }
    }
    // </fencing>

    vlog(clusterlog.info, "SHAI: fenced");

    // <check> if there is there is inflight abort
    //         or tx already is in prepared state
    //         or if term has changes and state is stale
    if (!_mem_state.expected.contains(bid.pid)) {
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }
    // </check>
    if (_mem_state.has_prepare_applied.contains(bid.pid)) {
        // we already trying to prepare that tx so it can't
        // accept new produce requests, impossible situation
        // should log
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }
    
    // <check> _mem_state.estimated has value when there is a follow up produce
    //         when first hasn't finished yet, since we use overwrite 
    //         ack=1 in-tx produce replicate should be very fast and
    //         this is semi impossible situation
    if (_mem_state.estimated.contains(bid.pid)) {
        // unexpected situation preventing tx from
        // preparing / comitting / replicating
        _mem_state.expected.erase(bid.pid);
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }
    // </check>

    if (!_mem_state.tx_start.contains(bid.pid)) {
        _mem_state.estimated.emplace(bid.pid, _insync_offset);
    }

    // after continuation _mem_state may change so caching term
    // to invalidate the post processing
    auto term = _mem_state.term;

    vlog(clusterlog.info, "SHAI: replicating");

    auto r = co_await _c->replicate(term, std::move(br), raft::replicate_options(raft::consistency_level::leader_ack));
    vlog(clusterlog.info, "SHAI: got r");
    if (!r) {
        if (_mem_state.estimated.contains(bid.pid)) {
            // unexpected situation preventing tx from
            // preparing / comitting / replicating
            _mem_state.expected.erase(bid.pid);
        }
        co_return checked<raft::replicate_result, kafka::error_code>(kafka::error_code::unknown_server_error);
    }

    vlog(clusterlog.info, "SHAI: postprocess");

    auto b = r.value();

    if (_mem_state.term != term) {
        co_return checked<raft::replicate_result, kafka::error_code>(b);
    }

    auto last_offset = model::offset(b.last_offset());
    if (!_mem_state.tx_start.contains(bid.pid)) {
        auto base_offset = model::offset(last_offset() - (bid.record_count - 1));
        _mem_state.tx_start.emplace(bid.pid, base_offset);
        _mem_state.tx_starts.insert(base_offset);
        _mem_state.estimated.erase(bid.pid);
    }
    co_return checked<raft::replicate_result, kafka::error_code>(b);*/
}

std::optional<model::offset>
tx_stm::tx_lso() {
    std::optional<model::offset> min = std::nullopt;
    
    if (!_log_state.ongoing_set.empty()) {
        min = std::optional<model::offset>(*_log_state.ongoing_set.begin());
    }

    if (!_mem_state.tx_starts.empty()) {
        if (!min.has_value()) {
            min = std::optional<model::offset>(*_mem_state.tx_starts.begin());
        }
        if (*_mem_state.tx_starts.begin() < min.value()) {
            min = std::optional<model::offset>(*_mem_state.tx_starts.begin());
        }
    }

    for (auto& entry : _mem_state.estimated) {
        if (!min.has_value()) {
            min = std::optional<model::offset>(entry.second);
        }
        if (entry.second < min.value()) {
            min = std::optional<model::offset>(entry.second);
        }
    }

    if (min.has_value()) {
        return std::optional<model::offset>(min.value()-1);
    } else {
        return std::optional<model::offset>(_insync_offset);
    }
}

std::vector<tx_stm::tx_range>
tx_stm::aborted_transactions(model::offset from, model::offset to) {
    std::vector<tx_stm::tx_range> result;
    for (auto& range : _log_state.aborted) {
        if (range.second.last < from) {
            continue;
        }
        if (range.second.first > to) {
            continue;
        }
        result.push_back(range.second);
    }
    return result;
}

void tx_stm::compact_snapshot() { }

ss::future<> tx_stm::apply(model::record_batch b) {
    vlog(clusterlog.info, "SHAI: applying");
    auto offset = b.last_offset();
    replay(std::move(b));
    _insync_offset = offset;
    return ss::now();
}

void tx_stm::replay(model::record_batch&& b) {
    vlog(clusterlog.info, "SHAI: replaying");
    const auto& hdr = b.header();
    auto bid = model::batch_identity::from(hdr);
    auto pid = bid.pid;

    if (hdr.type == tx_fence_batch_type) {
        vlog(clusterlog.info, "SHAI: fence batch");
        auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
        if (fence_it == _log_state.fence_pid_epoch.end()) {
            _log_state.fence_pid_epoch.emplace(id(pid), epoch(pid));
        } else if (fence_it->second < epoch(pid)) {
            fence_it->second=epoch(pid);
        }
        return;
    }
    
    if (hdr.type == tx_prepare_batch_type) {
        vlog(clusterlog.info, "SHAI: prepare batch");
        auto has_applied_it = _mem_state.has_prepare_applied.find(pid);
        
        auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
        if (fence_it == _log_state.fence_pid_epoch.end()) {
            // todo: add loggin since imposible situation
            // kafka clients replicates after add_to_partition
            // add_to_partition acks after begin
            // begin updates fencing
            _log_state.fence_pid_epoch.emplace(id(pid), epoch(pid));
        } else if (fence_it->second < epoch(pid)) {
            // todo: add loggin since imposible situation
            // kafka clients replicates after add_to_partition
            // add_to_partition acks after begin
            // begin updates fencing
            fence_it->second=epoch(pid);
        } else if (fence_it->second > epoch(pid)) {
            // prepare failed with explicit reject
            // and can't be prepared in the future
            // blocking potential produce and prepare 
            // requests
            _mem_state.expected.erase(pid);
            return;
        }

        if (_log_state.aborted.contains(pid)) {
            // can't prepare aborted requests
            // prepare failed with explicit reject
            // and can't be prepared in the future
            // blocking potential produce and prepare 
            // requests
            _mem_state.expected.erase(pid);
            return;
        }

        // check if duplicated prepare issued during
        // recovery (see tm_stm)
        if (!_log_state.prepared.contains(pid)) {
            _log_state.prepared.insert(pid);
        }
        _mem_state.expected.erase(pid);
        
        if (has_applied_it != _mem_state.has_prepare_applied.end()) {
            has_applied_it->second = true;
        }
        return;
    }

    if (hdr.type != raft::data_batch_type) {
        return;
    }
    
    if (hdr.attrs.is_control()) {
        vlog(clusterlog.info, "SHAI: control batch");
        vassert(b.record_count() == 1, "control record must contain a single record");
        auto r = b.copy_records();
        auto& record = *r.begin();
        auto key = record.release_key();
        kafka::request_reader key_reader(std::move(key));
        auto version = model::control_record_version(key_reader.read_int16());
        vassert(version == model::current_control_record_version, "unknown control record version");
        
        auto fence_it = _log_state.fence_pid_epoch.find(id(pid));
        if (fence_it == _log_state.fence_pid_epoch.end()) {
            _log_state.fence_pid_epoch.emplace(id(pid), epoch(pid));
        } else if (fence_it->second < epoch(pid)) {
            fence_it->second=epoch(pid);
        } else if (fence_it->second > epoch(pid)) {
            // we don't fence off aborts and commits
            // because tx coordinator already decide
            // a tx outcome and acked it to the client
            // and there is no way to rollback it
        }
        
        auto crt = model::control_record_type(key_reader.read_int16());
        if (crt == model::control_record_type::tx_abort) {
            // check if it's already aborted it's ok to
            // re-abort aborted txes see tm_stm recovery
            // duting init_tx
            if (_log_state.aborted.contains(pid)) {
                return;
            }

            if (!_log_state.prepared.contains(pid)) {
                // we either trying to abort an already
                // comitted tx or a tx which was never
                // prepared both
                // this is an invariant violation:
                // log, vassert or ignore
                // so far falling down to ignore
            }        

            // soly rely on log data because up to this
            // point (apply(abort)) all writes which came
            // before it already materialized there
            _log_state.prepared.erase(pid);
            auto offset_it = _log_state.ongoing_map.find(pid);
            if (offset_it != _log_state.ongoing_map.end()) {
                _log_state.aborted.emplace(pid, offset_it->second);
                _log_state.ongoing_set.erase(offset_it->second.first);
                _log_state.ongoing_map.erase(pid);
            }

            // cleaning mem state
            _mem_state.expected.erase(pid);
            _mem_state.estimated.erase(pid);
            _mem_state.has_prepare_applied.erase(pid);
            auto tx_start_it = _mem_state.tx_start.find(pid);
            if (tx_start_it != _mem_state.tx_start.end()) {
                _mem_state.tx_starts.erase(tx_start_it->second);
                _mem_state.tx_start.erase(pid);
            }
        }
        
        if (crt == model::control_record_type::tx_commit) {
            // commit prepared => ok
            // commit empty => ok
            // commit aborted => error (log, vassert, ignore)
            // commit ongoing => error (log, vassert, ignore)

            if (_log_state.aborted.contains(pid)) {
                // log, vassert, ignore
            } else if (_log_state.prepared.contains(pid)) {
                // ok
            } else if (_log_state.ongoing_map.contains(pid)) {
                // log, vassert, ignore
            } 

            // mem.forget(pid)
            // log.forget(pid)

            _mem_state.has_prepare_applied.erase(pid);
            _log_state.prepared.erase(pid);
            auto offset_it = _log_state.ongoing_map.find(pid);
            if (offset_it != _log_state.ongoing_map.end()) {
                _log_state.ongoing_set.erase(offset_it->second.first);
                _log_state.ongoing_map.erase(pid);
            }
        }
        return;
    }
    
    if (hdr.attrs.is_transactional()) {
        vlog(clusterlog.info, "SHAI: transactional batch");
        auto last_offset = model::offset(b.last_offset());
        if (_log_state.aborted.contains(bid.pid)) {
            // log, vassert, ignore
            return;
        }
        if (_log_state.prepared.contains(bid.pid)) {
            // log, vassert, ignore
            return;
        }

        auto ongoing_it = _log_state.ongoing_map.find(bid.pid);
        if (ongoing_it != _log_state.ongoing_map.end()) {
            if (ongoing_it->second.last < last_offset) {
                ongoing_it->second.last = last_offset;
            }
        } else {
            auto base_offset = model::offset(last_offset() - (bid.record_count - 1));
            _log_state.ongoing_map.emplace(bid.pid, tx_range {
                .pid = bid.pid,
                .first = base_offset,
                .last = model::offset(last_offset)
            });
            _log_state.ongoing_set.insert(base_offset);
            _mem_state.estimated.erase(bid.pid);
        }
    }
}

void tx_stm::load_snapshot(stm_snapshot_header hdr, iobuf&& tx_ss_buf) {
    vassert(hdr.version == supported_version, "unsupported seq_snapshot_header version {}", hdr.version);
    iobuf_parser data_parser(std::move(tx_ss_buf));
    auto data = reflection::adl<tx_snapshot>{}.from(data_parser);

    for (auto& entry : data.fenced) {
        _log_state.fence_pid_epoch.emplace(id(entry), epoch(entry));
    }
    for (auto& entry : data.ongoing) {
        _log_state.ongoing_map.emplace(entry.pid, entry);
        _log_state.ongoing_set.insert(entry.first);
    }
    for (auto& entry : data.prepared) {
        _log_state.prepared.insert(entry);
    }
    for (auto& entry : data.aborted) {
        _log_state.aborted.emplace(entry.pid, entry);
    }
    
    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;
}

stm_snapshot tx_stm::take_snapshot() {
    tx_snapshot tx_ss;
    
    for (auto const& [k, v] : _log_state.fence_pid_epoch) {
        tx_ss.fenced.push_back(model::producer_identity {
            .id = k(),
            .epoch = v()
        });
    }
    for (auto& entry : _log_state.ongoing_map) {
        tx_ss.ongoing.push_back(entry.second);
    }
    for (auto& entry : _log_state.prepared) {
        tx_ss.prepared.push_back(entry);
    }
    for (auto& entry : _log_state.aborted) {
        tx_ss.aborted.push_back(entry.second);
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
