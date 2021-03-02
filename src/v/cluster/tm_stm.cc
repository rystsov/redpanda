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
tm_stm::try_change_status(kafka::transactional_id tx_id, int64_t etag, tm_transaction::tx_status status) {
    auto tx = _tx_table.find(tx_id);
    if (tx == _tx_table.end()) {
        return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::not_found));
    }
    if (tx->second.etag != etag) {
        return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::conflict));
    }
    tx->second.status = status;
    tx->second.etag = etag+1;
    
    return ss::make_ready_future<checked<tm_transaction, tm_stm::op_status>>(checked<tm_transaction, tm_stm::op_status>(tx->second));
}

checked<tm_transaction, tm_stm::op_status>
tm_stm::mark_tx_finished(kafka::transactional_id tx_id, int64_t etag) {
    auto tx = _tx_table.find(tx_id);
    if (tx == _tx_table.end()) {
        return checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::not_found);
    }
    if (tx->second.etag != etag) {
        return checked<tm_transaction, tm_stm::op_status>(tm_stm::op_status::conflict);
    }
    tx->second.status = tm_transaction::tx_status::finished;
    tx->second.etag = etag+1;
    tx->second.partitions = std::vector<tm_transaction::rm>();
    return checked<tm_transaction, tm_stm::op_status>(tx->second);
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
    tx->second.partitions = std::vector<tm_transaction::rm>();
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

bool
tm_stm::add_partitions(kafka::transactional_id tx_id, int64_t etag, std::vector<tm_transaction::rm> partitions) {
    auto tx = _tx_table.find(tx_id);
    if (tx == _tx_table.end()) {
        return false;
    }
    if (tx->second.etag != etag) {
        return false;
    }
    tx->second.etag += 1;
    for (auto& partition : partitions) {
        tx->second.partitions.push_back(partition);
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
    _insync_offset = b.last_offset();

    compact_snapshot();
    
    return ss::now();
}

} // namespace cluster
