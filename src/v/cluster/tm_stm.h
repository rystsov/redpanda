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

#include <absl/container/flat_hash_map.h>

namespace cluster {

struct tm_snapshot {
    model::offset offset;
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
    int64_t etag;
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
    ss::future<checked<tm_transaction, tm_stm::op_status>> try_change_status(kafka::transactional_id, int64_t, tm_transaction::tx_status);
    checked<tm_transaction, tm_stm::op_status> mark_tx_finished(kafka::transactional_id, int64_t);
    ss::future<tm_stm::op_status> re_register_producer(kafka::transactional_id, int64_t, model::producer_identity);
    ss::future<tm_stm::op_status> register_new_producer(kafka::transactional_id, model::producer_identity);
    bool add_partitions(kafka::transactional_id, int64_t, std::vector<tm_transaction::rm>);

protected:
    void load_snapshot(stm_snapshot_header, iobuf&&) override;
    stm_snapshot take_snapshot() override;

    void compact_snapshot();

    absl::flat_hash_map<kafka::transactional_id, tm_transaction> _tx_table;
    ss::future<> apply(model::record_batch b) override;
};

} // namespace raft
