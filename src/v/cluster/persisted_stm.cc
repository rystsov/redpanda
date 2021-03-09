// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/persisted_stm.h"

#include "cluster/logger.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>

#include <filesystem>

namespace cluster {

persisted_stm::persisted_stm(
  const char* snapshot_mgr_name, ss::logger& logger, raft::consensus* c, config::configuration& config)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c)
  , _snapshot_mgr(
      std::filesystem::path(c->log_config().work_directory()),
      snapshot_mgr_name,
      ss::default_priority_class())
  , _log(logger)
  , _config(config) {}

ss::future<>
persisted_stm::hydrate_snapshot(storage::snapshot_reader& reader) {
    return reader.read_metadata().then([this, &reader](iobuf meta_buf) {
        iobuf_parser meta_parser(std::move(meta_buf));
        stm_snapshot_header hdr;
        hdr.version = reflection::adl<int8_t>{}.from(meta_parser);
        hdr.snapshot_size = reflection::adl<int32_t>{}.from(meta_parser);

        return read_iobuf_exactly(reader.input(), hdr.snapshot_size)
          .then([this, hdr](iobuf data_buf) {
              load_snapshot(hdr, std::move(data_buf));
          })
          .then([this]() { return _snapshot_mgr.remove_partial_snapshots(); });
    });
}

ss::future<>
persisted_stm::wait_for_snapshot_hydrated() {
    auto f = ss::now();
    if (unlikely(!_resolved_when_snapshot_hydrated.available())) {
        f = _resolved_when_snapshot_hydrated.get_shared_future();
    }
    return f;
}

ss::future<>
persisted_stm::persist_snapshot(stm_snapshot&& snapshot) {
    iobuf data_size_buf;
    reflection::serialize(data_size_buf, snapshot.header.version, snapshot.header.snapshot_size);

    return _snapshot_mgr.start_snapshot().then(
      [this, data = std::move(snapshot.data), data_size_buf = std::move(data_size_buf)](
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

ss::future<>
persisted_stm::do_make_snapshot() {
    vlog(clusterlog.trace, "saving persisted_stm snapshot");
    auto snapshot = take_snapshot();
    auto offset = snapshot.offset;

    return persist_snapshot(std::move(snapshot))
      .then([this, offset]() {
          if (offset >= _last_snapshot_offset) {
              _last_snapshot_offset = offset;
          }
      });
}

ss::future<>
persisted_stm::make_snapshot() {
    return _op_lock.with([this]() {
        auto f = wait_for_snapshot_hydrated();
        return f.then([this] { return do_make_snapshot(); });
    });
}

ss::future<>
persisted_stm::ensure_snapshot_exists(model::offset target_offset) {
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

ss::future<>
persisted_stm::catchup(model::term_id last_term, model::offset last_offset) {
    vlog(
      _log.trace,
      "persisted_stm is catching up with term:{} and offset:{}",
      last_term,
      last_offset);
    return wait(last_offset, model::no_timeout)
      .then([this, last_offset, last_term]() {
          auto meta = _c->meta();
          vlog(
            _log.trace,
            "persisted_stm caught up with term:{} and offset:{}; current_term: {} "
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

ss::future<bool> 
persisted_stm::sync(model::timeout_clock::duration timeout) {
    if (!_c->is_leader()) {
        return ss::make_ready_future<bool>(false);
    }
    if (_insync_term == _c->term()) {
        return ss::make_ready_future<bool>(true);
    }
    if (_is_catching_up) {
        auto deadline = model::timeout_clock::now() + timeout;
        auto sync_waiter = ss::make_lw_shared<expiring_promise<bool>>();
        _sync_waiters.push_back(sync_waiter);
        return sync_waiter->get_future_with_timeout(deadline, [](){ return false; });
    }
    _is_catching_up = true;
    auto term = _c->term();
    return quorum_write_empty_batch(model::timeout_clock::now() + timeout).then([](auto r) {
        if (r) {
            return true;
        }
        return false;
    }).handle_exception([]([[maybe_unused]] std::exception_ptr e){
        return false;
    }).then([this, term](auto is_synced) {
        is_synced = is_synced && (term == _c->term());
        if (is_synced) {
            _insync_term = term;
        }
        for (auto& sync_waiter : _sync_waiters) {
            if (!sync_waiter->available()) {
                sync_waiter->set_value(is_synced);
            }
        }
        _sync_waiters.clear();
        return is_synced;
    });
}

// 

ss::future<>
persisted_stm::catchup() {
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

ss::future<>
persisted_stm::start() {
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

} // namespace cluster
