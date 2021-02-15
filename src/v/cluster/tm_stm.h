/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "raft/types.h"
#include "storage/snapshot.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"
#include "cluster/persisted_stm.h"
#include <compare>

#include <absl/container/flat_hash_map.h>

namespace cluster {

struct tm_etag {
    int64_t log_etag {0};
    int64_t mem_etag {0};

    tm_etag inc_mem() const {
        return tm_etag {
            .log_etag = this->log_etag,
            .mem_etag = this->mem_etag + 1
        };
    }

    tm_etag inc_log() const {
        return tm_etag {
            .log_etag = this->log_etag + 1,
            .mem_etag = 0
        };
    }

    auto operator<=>(const tm_etag&) const = default;
};

struct tm_transaction {
    enum tx_status {
        ongoing,
        preparing,
        prepared,
        aborting,
        finished,
    };

    struct rm {
        model::ntp ntp;
        model::term_id etag;
    };

    kafka::transactional_id id;
    tx_status status;
    model::producer_identity pid;
    // TODO: make an ntp map 
    std::vector<rm> partitions;
    tm_etag etag;
    // transactional_id identifies an application
    // producer_identity identifies a session
    // tx_seq is a transaction identifier
    model::tx_seq tx_seq;

    model::term_id update_term;
};

struct tm_snapshot {
    model::offset offset;
    std::vector<tm_transaction> transactions;
};


class tm_stm final
  : public persisted_stm
{
public:
    static constexpr const int8_t supported_version = 0;

    enum op_status {
        success,
        not_found,
        conflict,
        unknown,
    };

    explicit tm_stm(ss::logger&, raft::consensus*, config::configuration&);

    std::optional<tm_transaction> get_tx(kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>> try_change_status(kafka::transactional_id, tm_etag, tm_transaction::tx_status);
    checked<tm_transaction, tm_stm::op_status> mark_tx_finished(kafka::transactional_id, tm_etag);
    checked<tm_transaction, tm_stm::op_status> mark_tx_ongoing(kafka::transactional_id, tm_etag);
    ss::future<tm_stm::op_status> re_register_producer(kafka::transactional_id, tm_etag, model::producer_identity);
    ss::future<tm_stm::op_status> register_new_producer(kafka::transactional_id, model::producer_identity);
    bool add_partitions(kafka::transactional_id, tm_etag, std::vector<tm_transaction::rm>);

    ss::lw_shared_ptr<mutex> get_end_lock(kafka::transactional_id tid) {
        return _end_locks.find(tid)->second;
    }

protected:
    struct tx_updated_cmd {
        static constexpr uint8_t record_key = 0;
        tm_transaction tx;
        tm_etag prev_etag;
    };

    void load_snapshot(stm_snapshot_header, iobuf&&) override;
    stm_snapshot take_snapshot() override;

    void compact_snapshot();

    absl::flat_hash_map<kafka::transactional_id, tm_transaction> _tx_table;
    absl::flat_hash_map<kafka::transactional_id, ss::lw_shared_ptr<mutex>> _tx_locks;
    absl::flat_hash_map<kafka::transactional_id, ss::lw_shared_ptr<mutex>> _end_locks;
    ss::future<> apply(model::record_batch b) override;

private:
    ss::future<tm_stm::op_status> register_new_producer(model::term_id, kafka::transactional_id, model::producer_identity);
    ss::future<checked<tm_transaction, tm_stm::op_status>> save_tx(model::term_id, tm_transaction);
    ss::future<result<raft::replicate_result>> replicate_quorum_ack(model::term_id term, model::record_batch&& batch) {
        return _c->replicate(
            term,
            model::make_memory_record_batch_reader(std::move(batch)),
            raft::replicate_options{raft::consistency_level::quorum_ack});
    }
};

} // namespace raft
