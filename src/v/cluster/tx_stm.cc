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
#include "raft/types.h"
#include "storage/record_batch_builder.h"
#include "storage/parser_utils.h"
#include "model/record.h"

#include <seastar/core/future.hh>

#include <filesystem>

namespace cluster {

tx_stm::tx_stm(
  ss::logger& logger, raft::consensus* c, [[maybe_unused]] config::configuration& config)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c)
  , _snapshot_mgr(
      std::filesystem::path(c->log_config().work_directory()),
      "tx",
      ss::default_priority_class())
  , _log(logger) {}

ss::future<> tx_stm::hydrate_snapshot(storage::snapshot_reader& reader) {
    return reader.read_metadata().then([this, &reader](iobuf meta_buf) {
        iobuf_parser meta_parser(std::move(meta_buf));
        tx_snapshot_header hdr;
        hdr.version = reflection::adl<int8_t>{}.from(meta_parser);
        vassert(
          hdr.version == tx_snapshot_header::supported_version,
          "unsupported tx_snapshot_header version {}",
          hdr.version);
        hdr.snapshot_size = reflection::adl<int32_t>{}.from(meta_parser);

        return read_iobuf_exactly(reader.input(), hdr.snapshot_size)
          .then([this](iobuf data_buf) {
              iobuf_parser data_parser(std::move(data_buf));
              auto data = reflection::adl<snapshot>{}.from(data_parser);
              // data recovery
              _last_snapshot_offset = data.offset;
              _insync_offset = data.offset;
              vlog(
                clusterlog.trace,
                "tx_stm snapshot with offset:{} is loaded",
                data.offset);
          })
          .then([this]() { return _snapshot_mgr.remove_partial_snapshots(); });
    });
}

ss::future<> tx_stm::wait_for_snapshot_hydrated() {
    auto f = ss::now();
    if (unlikely(!_resolved_when_snapshot_hydrated.available())) {
        f = _resolved_when_snapshot_hydrated.get_shared_future();
    }
    return f;
}

ss::future<> tx_stm::persist_snapshot(iobuf&& data) {
    tx_snapshot_header header;
    header.version = tx_snapshot_header::supported_version;
    header.snapshot_size = data.size_bytes();

    iobuf data_size_buf;
    reflection::serialize(data_size_buf, header.version, header.snapshot_size);

    return _snapshot_mgr.start_snapshot().then(
      [this, data = std::move(data), data_size_buf = std::move(data_size_buf)](
        storage::snapshot_writer writer) mutable {
          return ss::do_with(
            std::move(writer),
            [this,
             data = std::move(data),
             data_size_buf = std::move(data_size_buf)](
              storage::snapshot_writer& writer) mutable {
                return writer.write_metadata(std::move(data_size_buf))
                  .then([&writer, data = std::move(data)]() mutable {
                      return write_iobuf_to_output_stream(
                        std::move(data), writer.output());
                  })
                  .finally([&writer] { return writer.close(); })
                  .then([this, &writer] {
                      return _snapshot_mgr.finish_snapshot(writer);
                  });
            });
      });
}

ss::future<> tx_stm::make_snapshot() {
    return _op_lock.with([this]() {
        auto f = wait_for_snapshot_hydrated();
        return f.then([this] { return do_make_snapshot(); });
    });
}

ss::future<> tx_stm::ensure_snapshot_exists(model::offset target_offset) {
    return _op_lock.with([this, target_offset]() {
        auto f = wait_for_snapshot_hydrated();

        return f.then([this, target_offset] {
            if (target_offset <= _last_snapshot_offset) {
                return ss::now();
            }
            return wait(target_offset, model::no_timeout)
              .then([this, target_offset]() {
                  vassert(
                    target_offset <= _insync_offset,
                    "after we waited for target_offset ({}) _insync_offset "
                    "({}) should have matched it or bypassed",
                    target_offset,
                    _insync_offset);
                  return do_make_snapshot();
              });
        });
    });
}

ss::future<> tx_stm::do_make_snapshot() {
    vlog(clusterlog.trace, "saving tx_stm snapshot");
    snapshot data;
    data.offset = _insync_offset;
    // dump data

    iobuf v_buf;
    reflection::adl<snapshot>{}.to(v_buf, data);
    return persist_snapshot(std::move(v_buf))
      .then([this, snapshot_offset = data.offset]() {
          if (snapshot_offset >= _last_snapshot_offset) {
              _last_snapshot_offset = snapshot_offset;
          }
      });
}

ss::future<> tx_stm::start() {
    return _snapshot_mgr.open_snapshot().then(
      [this](std::optional<storage::snapshot_reader> reader) {
          auto f = ss::now();
          if (reader) {
              f = ss::do_with(
                std::move(*reader), [this](storage::snapshot_reader& reader) {
                    return hydrate_snapshot(reader).finally([this, &reader] {
                        auto offset = std::max(
                          _insync_offset, _c->start_offset());
                        if (offset >= model::offset(0)) {
                            set_next(offset);
                        }
                        _resolved_when_snapshot_hydrated.set_value();
                        return reader.close();
                    });
                });
          } else {
              auto offset = _c->start_offset();
              if (offset >= model::offset(0)) {
                  set_next(offset);
              }
              _resolved_when_snapshot_hydrated.set_value();
          }

          return f.then([this]() { return state_machine::start(); })
            .then([this]() {
                auto offset = _c->meta().commit_index;
                if (offset >= model::offset(0)) {
                    (void)ss::with_gate(_gate, [this, offset] {
                        // saving a snapshot after catchup with the tip of the
                        // log
                        return ensure_snapshot_exists(offset);
                    });
                }
            });
      });
}

ss::future<> tx_stm::catchup() {
    if (_insync_term != _c->term()) {
        if (!_is_catching_up) {
            _is_catching_up = true;
            auto meta = _c->meta();
            return catchup(meta.prev_log_term, meta.prev_log_index)
              .then([this]() { _is_catching_up = false; });
        }
    }
    return ss::now();
}

static iobuf make_control_record_key(model::control_record_type crt) {
    iobuf b;
    kafka::response_writer w(b);
    w.write(model::current_control_record_version);
    w.write(crt);
    return b;
}

static model::record make_control_record(model::control_record_type crt) {
    int index = 0;
    static constexpr size_t zero_vint_size = vint::vint_size(0);
    auto k = make_control_record_key(crt);
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

static model::record_batch make_control_record_batch(model::producer_identity pid, model::control_record_type crt) {
    auto ts = model::timestamp::now()();
    auto header = model::record_batch_header{
      .size_bytes = 0, // computed later
      .base_offset = model::offset(0), // ?
      .type = raft::data_batch_type,
      .crc = 0, // we-reassign later
      .attrs = model::record_batch_attributes(model::record_batch_attributes::control_mask),
      .last_offset_delta = 0,
      .first_timestamp = model::timestamp(ts),
      .max_timestamp = model::timestamp(ts),
      .producer_id = pid.id,
      .producer_epoch = pid.epoch,
      .base_sequence = 0,
      .record_count = 1};
    
    iobuf records;
    model::append_record_to_buffer(
        records,
        make_control_record(crt));
    storage::internal::reset_size_checksum_metadata(header, records);
    return model::record_batch(header, std::move(records), model::record_batch::tag_ctor_ng{});
}

ss::future<tx_errc>
tx_stm::abort_tx([[maybe_unused]] model::producer_identity pid, [[maybe_unused]] model::timeout_clock::time_point timeout) {
    auto batch = make_control_record_batch(pid, model::control_record_type::tx_abort);

    vlog(clusterlog.info, "aborting tx");
    return _c->replicate(
        model::make_memory_record_batch_reader(std::move(batch)),
        raft::replicate_options(raft::consistency_level::quorum_ack))
      .then([](result<raft::replicate_result> r) {
          if (r) {
              return tx_errc::success;
          }
          return tx_errc::timeout;
      });
}

ss::future<tx_errc>
tx_stm::prepare_tx(model::term_id etag, [[maybe_unused]] model::partition_id tm, [[maybe_unused]] model::producer_identity pid, [[maybe_unused]] model::timeout_clock::time_point timeout) {
    if (!_c->is_leader()) {
        return ss::make_ready_future<tx_errc>(tx_errc::conflict);
    }
    if (_insync_term != etag) {
        return ss::make_ready_future<tx_errc>(tx_errc::conflict);
    }
    
    return ss::make_ready_future<tx_errc>(tx_errc::success);
}

ss::future<tx_errc>
tx_stm::commit_tx(model::producer_identity pid, [[maybe_unused]] model::timeout_clock::time_point timeout) {
    auto batch = make_control_record_batch(pid, model::control_record_type::tx_commit);

    vlog(clusterlog.info, "comitting tx");
    return _c->replicate(
        model::make_memory_record_batch_reader(std::move(batch)),
        raft::replicate_options(raft::consistency_level::quorum_ack))
      .then([](result<raft::replicate_result> r) {
          if (r) {
              return tx_errc::success;
          }
          return tx_errc::timeout;
      });
}

std::optional<model::term_id>
tx_stm::begin_tx() {
    if (_c->is_leader()) {
        return std::optional<model::term_id>(_insync_term);
    }
    return std::nullopt;
}

ss::future<checked<raft::replicate_result, kafka::error_code>>
tx_stm::replicate(
  [[maybe_unused]] model::batch_identity bid,
  model::record_batch_reader&& r,
  [[maybe_unused]] raft::replicate_options opts) {
    vlog(clusterlog.info, "tx replicating");
    return _c->replicate(std::move(r), raft::replicate_options(raft::consistency_level::leader_ack))
      .then([](result<raft::replicate_result> r) {
          if (r) {
              return checked<raft::replicate_result, kafka::error_code>(r.value());
          }
          return checked<raft::replicate_result, kafka::error_code>(
            kafka::error_code::unknown_server_error);
      });
}

ss::future<>
tx_stm::catchup(model::term_id last_term, model::offset last_offset) {
    vlog(
      _log.trace,
      "tx_stm is catching up with term:{} and offset:{}",
      last_term,
      last_offset);
    return wait(last_offset, model::no_timeout)
      .then([this, last_offset, last_term]() {
          auto meta = _c->meta();
          vlog(
            _log.trace,
            "tx_stm caught up with term:{} and offset:{}; current_term: {} "
            "current_offset: {} dirty_term: {} dirty_offset: {}",
            last_term,
            last_offset,
            meta.term,
            meta.commit_index,
            meta.prev_log_term,
            meta.prev_log_index);
          if (last_term <= meta.term) {
              _insync_term = last_term;
          }
      });
}

void tx_stm::compact_snapshot() { }

ss::future<> tx_stm::apply(model::record_batch b) {
    _insync_offset = b.last_offset();
    
    return ss::now();
}

} // namespace raft
