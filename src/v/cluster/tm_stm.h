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

#include <absl/container/flat_hash_map.h>

namespace cluster {

struct tm_snapshot_header {
    static constexpr const int8_t supported_version = 0;

    int8_t version{tm_snapshot_header::supported_version};
    int32_t snapshot_size{0};

    static constexpr const size_t ondisk_size = sizeof(version)
                                                + sizeof(snapshot_size);
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
    std::vector<rm> partitions;
    int64_t etag;
};

class tm_stm final
  : public raft::state_machine
  , public storage::snapshotable_stm {
public:
    enum op_status {
        success,
        not_found,
        conflict,
        unknown,
    };

    explicit tm_stm(ss::logger&, raft::consensus*, config::configuration&);

    ss::future<> start() final;

    ss::future<> ensure_snapshot_exists(model::offset) final;
    ss::future<> make_snapshot() final;
    ss::future<> catchup();

    std::optional<tm_transaction> get_tx(kafka::transactional_id);
    ss::future<checked<tm_transaction, tm_stm::op_status>> try_change_status(kafka::transactional_id, int64_t, tm_transaction::tx_status);
    ss::future<tm_stm::op_status> re_register_producer(kafka::transactional_id, int64_t, model::producer_identity);
    ss::future<tm_stm::op_status> register_new_producer(kafka::transactional_id, model::producer_identity);

private:
    struct snapshot {
        model::offset offset;
    };

    ss::future<> do_make_snapshot();
    ss::future<> hydrate_snapshot(storage::snapshot_reader&);

    ss::future<> wait_for_snapshot_hydrated();
    ss::future<> persist_snapshot(iobuf&& data);
    void compact_snapshot();

    ss::future<> catchup(model::term_id, model::offset);

    model::offset _last_snapshot_offset;
    mutex _op_lock;
    ss::shared_promise<> _resolved_when_snapshot_hydrated;
    bool _is_catching_up{false};
    model::term_id _insync_term{-1};
    model::offset _insync_offset{-1};
    raft::consensus* _c;
    storage::snapshot_manager _snapshot_mgr;
    ss::logger& _log;
    ss::future<> apply(model::record_batch b) override;
};

} // namespace raft
