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

struct persisted_snapshot_header {
    static constexpr const int8_t supported_version = 0;

    int8_t version{persisted_snapshot_header::supported_version};
    int32_t snapshot_size{0};

    static constexpr const size_t ondisk_size = sizeof(version)
                                                + sizeof(snapshot_size);
};

/**
 * persisted_stm is a layer in front of the log responsible for maintaining
 * idempotency of the producers. It works by keeping track of the last
 * event within a session (identified by a producer id and a epoch)
 * and validating that the next event goes strictly after a current
 * event where the precedence is defined by sequence number set by
 * clients.
 *
 * Sequence numbers are part of the events and persisted in a
 * log. To avoid scanning the whole log to reconstruct a map from
 * sessions to its last sequence number persisted_stm uses snapshotting.
 *
 * The potential divergence of the session - sequence map is prevented
 * by using conditional replicate and forcing cache refresh when
 * a expected term doesn't match the current term thus avoiding the
 * ABA problem where A and B correspond to different nodes holding
 * a leadership.
 */

template <class snapshot, class snapshot_header>
class persisted_stm
  : public raft::state_machine
  , public storage::snapshotable_stm {
public:
    explicit persisted_stm(ss::logger&, raft::consensus*, config::configuration&);

    ss::future<> make_snapshot() final;
    ss::future<> ensure_snapshot_exists(model::offset) final;
    ss::future<> catchup();
    
    ss::future<> start() override;

protected:
    virtual void load_snapshot(snapshot&);
    virtual snapshot take_snapshot();
    ss::future<> hydrate_snapshot(storage::snapshot_reader&);
    ss::future<> wait_for_snapshot_hydrated();
    ss::future<> persist_snapshot(iobuf&& data);
    ss::future<> do_make_snapshot();

    ss::future<> catchup(model::term_id, model::offset);
    
    /*struct persisted_entry {
        model::producer_identity pid;
        int32_t seq;
        model::timestamp::type last_write_timestamp;
    };

    struct snapshot {
        model::offset offset;
        std::vector<persisted_entry> entries;
    };

    
    

    
    
    void compact_snapshot();

    

    absl::flat_hash_map<model::producer_identity, persisted_entry> _persisted_table;
    
    model::timestamp _oldest_session;
    */
    mutex _op_lock;
    ss::shared_promise<> _resolved_when_snapshot_hydrated;
    model::offset _last_snapshot_offset;
    bool _is_catching_up{false};
    model::term_id _insync_term{-1};
    model::offset _insync_offset{-1};
    raft::consensus* _c;
    storage::snapshot_manager _snapshot_mgr;
    ss::logger& _log;
    config::configuration& _config;
    // ss::future<> apply(model::record_batch b) override;
};

} // namespace cluster
