// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"

#include "raft/errc.h"
#include "cluster/logger.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>

#include <filesystem>
#include <optional>

namespace cluster {

tm_stm::tm_stm(
  ss::logger& logger, raft::consensus* c, [[maybe_unused]] config::configuration& config)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c)
  , _snapshot_mgr(
      std::filesystem::path(c->log_config().work_directory()),
      "tx",
      ss::default_priority_class())
  , _log(logger) {}

ss::future<> tm_stm::hydrate_snapshot(storage::snapshot_reader& reader) {
    return reader.read_metadata().then([this, &reader](iobuf meta_buf) {
        iobuf_parser meta_parser(std::move(meta_buf));
        tm_snapshot_header hdr;
        hdr.version = reflection::adl<int8_t>{}.from(meta_parser);
        vassert(
          hdr.version == tm_snapshot_header::supported_version,
          "unsupported tm_snapshot_header version {}",
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
                "tm_stm snapshot with offset:{} is loaded",
                data.offset);
          })
          .then([this]() { return _snapshot_mgr.remove_partial_snapshots(); });
    });
}

ss::future<> tm_stm::wait_for_snapshot_hydrated() {
    auto f = ss::now();
    if (unlikely(!_resolved_when_snapshot_hydrated.available())) {
        f = _resolved_when_snapshot_hydrated.get_shared_future();
    }
    return f;
}

ss::future<> tm_stm::persist_snapshot(iobuf&& data) {
    tm_snapshot_header header;
    header.version = tm_snapshot_header::supported_version;
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

ss::future<> tm_stm::make_snapshot() {
    return _op_lock.with([this]() {
        auto f = wait_for_snapshot_hydrated();
        return f.then([this] { return do_make_snapshot(); });
    });
}

ss::future<> tm_stm::ensure_snapshot_exists(model::offset target_offset) {
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

ss::future<> tm_stm::do_make_snapshot() {
    vlog(clusterlog.trace, "saving tm_stm snapshot");
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

ss::future<> tm_stm::start() {
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

ss::future<> tm_stm::catchup() {
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

std::optional<tm_transaction>
tm_stm::get_tx([[maybe_unused]] kafka::transactional_id tx_id) {
    return std::nullopt;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::try_change_status([[maybe_unused]] kafka::transactional_id tx_id, [[maybe_unused]] int64_t etag, [[maybe_unused]] tm_transaction::tx_status status) {
    return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::not_found));
}

ss::future<tm_stm::op_status>
tm_stm::re_register_producer(kafka::transactional_id tx_id, int64_t etag, model::producer_identity pid) {
    auto tx = _tx_table.find(tx_id);
    if (tx == _tx_table.end()) {
        return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::not_found);
    }
    if (tx->second.etag != etag) {
        return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::conflict);
    }
    tx->second.status = tm_transaction::tx_status::ongoing;
    tx->second.pid = pid;
    tx->second.etag = etag+1;
    return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::success);
}

ss::future<tm_stm::op_status>
tm_stm::register_new_producer(kafka::transactional_id tx_id, model::producer_identity pid) {
    auto tx = _tx_table.find(tx_id);
    if (tx != _tx_table.end()) {
        return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::conflict);
    }
    tm_transaction entry {
        .id = tx_id,
        .status = tm_transaction::tx_status::ongoing,
        .pid = pid,
        .etag = 0
    };
    _tx_table.emplace(entry.id, entry);
    return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::success);
}

ss::future<>
tm_stm::catchup(model::term_id last_term, model::offset last_offset) {
    vlog(
      _log.trace,
      "tm_stm is catching up with term:{} and offset:{}",
      last_term,
      last_offset);
    return wait(last_offset, model::no_timeout)
      .then([this, last_offset, last_term]() {
          auto meta = _c->meta();
          vlog(
            _log.trace,
            "tm_stm caught up with term:{} and offset:{}; current_term: {} "
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

void tm_stm::compact_snapshot() { }

ss::future<> tm_stm::apply(model::record_batch b) {
    _insync_offset = b.last_offset();
    
    return ss::now();
}

} // namespace raft
