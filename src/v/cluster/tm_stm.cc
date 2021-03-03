// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"
#include "units.h"
#include "raft/errc.h"
#include "cluster/logger.h"
#include "raft/types.h"
#include "cluster/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>

#include <filesystem>
#include <optional>

namespace cluster {
using namespace std::chrono_literals;

template<typename T>
static model::record_batch serialize_cmd(T t, model::record_batch_type type) {
    storage::record_batch_builder b(type, model::offset(0));
    iobuf key_buf;
    reflection::adl<uint8_t>{}.to(key_buf, T::record_key);
    iobuf v_buf;
    reflection::adl<T>{}.to(v_buf, std::move(t));
    b.add_raw_kv(std::move(key_buf), std::move(v_buf));
    return std::move(b).build();
}

tm_stm::tm_stm(
  ss::logger& logger, raft::consensus* c, config::configuration& config)
  : persisted_stm("tm", logger, c, config)
{}

std::optional<tm_transaction>
tm_stm::get_tx(kafka::transactional_id tx_id) {
    auto tx = _tx_table.find(tx_id);
    if (tx != _tx_table.end()) {
        return std::optional<tm_transaction>(tx->second);
    }
    return std::nullopt;
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::save_tx(std::unique_ptr<mutex>& lock, model::term_id term, tm_transaction tx) {
    return lock->with([this, term, tx]() mutable {
        auto ptx = _tx_table.find(tx.id);
        if (ptx == _tx_table.end()) {
            return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::not_found));
        }
        if (ptx->second.etag != tx.etag) {
            return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::conflict));
        }

        auto etag = tx.etag;
        tx.etag = etag.inc_log();
        tx.update_term = term;
        tx_updated_cmd cmd {
            .tx = tx,
            .prev_etag = etag
        };
        auto batch = serialize_cmd(cmd, tm_stm_batch_type);

        return replicate_quorum_ack(term, std::move(batch)).then([this, term, tx_id = tx.id, new_etag = tx.etag](auto r){
            if (!r) {
                return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::unknown));
            }

            return wait(model::offset(r.value().last_offset()), model::no_timeout).then([this, term, tx_id, new_etag](){
                auto ptx = _tx_table.find(tx_id);
                if (ptx == _tx_table.end()) {
                    return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::conflict));
                }
                if (ptx->second.etag != new_etag) {
                    return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::conflict));
                }
                if (ptx->second.update_term != term) {
                    return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::conflict));
                }
                return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(ptx->second));
            });
        });
    });
}

ss::future<checked<tm_transaction, tm_stm::op_status>>
tm_stm::try_change_status(kafka::transactional_id tx_id, tm_etag etag, tm_transaction::tx_status status) {
    return is_caught_up(2'000ms).then([this, tx_id, etag, status](auto is_ready){
        if (!is_ready) {
            return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::unknown));
        }

        auto term = _insync_term;
        auto ptx = _tx_table.find(tx_id);
        if (ptx == _tx_table.end()) {
            return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::not_found));
        }
        if (ptx->second.etag != etag) {
            return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::conflict));
        }
        auto tx = ptx->second;
        tx.status = status;
        return save_tx(_tx_locks.find(tx_id)->second, term, tx);
    });
}

checked<tm_transaction, tm_stm::op_status>
tm_stm::mark_tx_finished(kafka::transactional_id tx_id, tm_etag etag) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        return checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::not_found);
    }
    if (ptx->second.etag != etag) {
        return checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::conflict);
    }
    ptx->second.status = tm_transaction::tx_status::finished;
    ptx->second.etag = etag.inc_mem();
    ptx->second.partitions = std::vector<tm_transaction::rm>();
    return checked<tm_transaction, tm_stm::op_status>(ptx->second);
}

ss::future<tm_stm::op_status>
tm_stm::re_register_producer(kafka::transactional_id tx_id, tm_etag etag, model::producer_identity pid) {
    return is_caught_up(2'000ms).then([this, tx_id, etag, pid](auto is_ready){
        if (!is_ready) {
            return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::unknown);
        }

        auto term = _insync_term;
        auto ptx = _tx_table.find(tx_id);
        if (ptx == _tx_table.end()) {
            return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::not_found);
        }
        if (ptx->second.etag != etag) {
            return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::conflict);
        }
        auto tx = ptx->second;
        tx.status = tm_transaction::tx_status::ongoing;
        tx.pid = pid;
        tx.partitions = std::vector<tm_transaction::rm>();
        return save_tx(_tx_locks.find(tx_id)->second, term, tx).then([](auto r) {
            if (!r.has_value()) {
                return tm_stm::op_status::unknown;
            }
            return tm_stm::op_status::success;
        });
    });
}

ss::future<tm_stm::op_status>
tm_stm::register_new_producer(kafka::transactional_id tx_id, model::producer_identity pid) {
    return is_caught_up(2'000ms).then([this, tx_id, pid](auto is_ready){
        if (!is_ready) {
            return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::unknown);
        }

        auto term = _insync_term;
        auto ptx = _tx_table.find(tx_id);
        if (ptx != _tx_table.end()) {
            return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::conflict);
        }
        auto lock_it = _tx_locks.find(tx_id);
        if (lock_it == _tx_locks.end()) {
            _tx_locks.emplace(tx_id, std::make_unique<mutex>());
            lock_it = _tx_locks.find(tx_id);
        }

        return lock_it->second->with([this, tx_id, term, pid]() {
            auto ptx = _tx_table.find(tx_id);
            if (ptx != _tx_table.end()) {
                return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::conflict);
            }

            auto tx = tm_transaction {
                .id = tx_id,
                .status = tm_transaction::tx_status::ongoing,
                .pid = pid,
                .update_term = term
            };
            tx_updated_cmd cmd {
                .tx = tx,
                .prev_etag = tx.etag
            };
            auto batch = serialize_cmd(cmd, tm_stm_batch_type);

            return replicate_quorum_ack(term, std::move(batch)).then([this, term, tx_id = tx.id, new_etag = tx.etag](auto r){
                if (!r) {
                    return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::unknown);
                }

                return wait(model::offset(r.value().last_offset()), model::no_timeout).then([this, term, tx_id, new_etag](){
                    auto ptx = _tx_table.find(tx_id);
                    if (ptx == _tx_table.end()) {
                        return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::conflict);
                    }
                    if (ptx->second.etag != new_etag) {
                        return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::conflict);
                    }
                    if (ptx->second.update_term != term) {
                        return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::conflict);
                    }
                    return ss::make_ready_future<tm_stm::op_status>(tm_stm::op_status::success);
                });
            });
        });
    });
}

bool
tm_stm::add_partitions(kafka::transactional_id tx_id, tm_etag etag, std::vector<tm_transaction::rm> partitions) {
    auto ptx = _tx_table.find(tx_id);
    if (ptx == _tx_table.end()) {
        return false;
    }
    if (ptx->second.etag != etag) {
        return false;
    }
    ptx->second.etag = etag.inc_mem();
    for (auto& partition : partitions) {
        ptx->second.partitions.push_back(partition);
    }
    return true;
}

void tm_stm::compact_snapshot() { }

void tm_stm::load_snapshot(stm_snapshot_header hdr, iobuf&& tm_ss_buf) {
    vassert(hdr.version == supported_version, "unsupported seq_snapshot_header version {}", hdr.version);
    iobuf_parser data_parser(std::move(tm_ss_buf));
    auto data = reflection::adl<tm_snapshot>{}.from(data_parser);
    
    for (auto& entry : data.transactions) {
        _tx_table.emplace(entry.id, entry);
    }
    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;
}

stm_snapshot tm_stm::take_snapshot() {
    tm_snapshot tm_ss;
    tm_ss.offset = _insync_offset;
    for (auto& entry : _tx_table) {
        tm_ss.transactions.push_back(entry.second);
    }

    iobuf tm_ss_buf;
    reflection::adl<tm_snapshot>{}.to(tm_ss_buf, tm_ss);
    
    stm_snapshot_header header;
    header.version = supported_version;
    header.snapshot_size = tm_ss_buf.size_bytes();

    stm_snapshot stm_ss;
    stm_ss.header = header;
    stm_ss.offset = _insync_offset;
    stm_ss.data = std::move(tm_ss_buf);
    return stm_ss;
}

ss::future<> tm_stm::apply(model::record_batch b) {
    const auto& hdr = b.header();

    if (hdr.type == tm_stm_batch_type) {
        
        vassert(b.record_count() == 1, "We expect single command in a batch of tm_stm_batch_type");
        auto r = b.copy_records();
        auto& record = *r.begin();
        auto rk = reflection::adl<uint8_t>{}.from(record.release_key());

        if (rk == tx_updated_cmd::record_key) {
            tx_updated_cmd cmd = reflection::adl<tx_updated_cmd>{}.from(record.release_value());
            auto ptx = _tx_table.find(cmd.tx.id);
            if (ptx == _tx_table.end()) {
                vassert(cmd.prev_etag == cmd.tx.etag, "First command should be a root");
                _tx_table.emplace(cmd.tx.id, cmd.tx);
                auto lock_it = _tx_locks.find(cmd.tx.id);
                if (lock_it == _tx_locks.end()) {
                    _tx_locks.emplace(cmd.tx.id, std::make_unique<mutex>());
                }
            } else {
                if (ptx->second.etag.log_etag == cmd.prev_etag.log_etag) {
                    ptx->second = cmd.tx;
                }
            }
        }
    }
    
    _insync_offset = b.last_offset();

    compact_snapshot();
    
    return ss::now();
}

} // namespace cluster
