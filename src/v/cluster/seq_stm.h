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
#include "cluster/persisted_stm.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

/**
 * seq_stm is a layer in front of the log responsible for maintaining
 * idempotency of the producers. It works by keeping track of the last
 * event within a session (identified by a producer id and a epoch)
 * and validating that the next event goes strictly after a current
 * event where the precedence is defined by sequence number set by
 * clients.
 *
 * Sequence numbers are part of the events and persisted in a
 * log. To avoid scanning the whole log to reconstruct a map from
 * sessions to its last sequence number seq_stm uses snapshotting.
 *
 * The potential divergence of the session - sequence map is prevented
 * by using conditional replicate and forcing cache refresh when
 * a expected term doesn't match the current term thus avoiding the
 * ABA problem where A and B correspond to different nodes holding
 * a leadership.
 */
class seq_stm final
  : public persisted_stm {
public:
    explicit seq_stm(ss::logger&, raft::consensus*, config::configuration&);
    static constexpr const int8_t supported_version = 0;

    ss::future<checked<raft::replicate_result, kafka::error_code>> replicate(
      model::batch_identity,
      model::record_batch_reader&&,
      raft::replicate_options);

protected:
    void load_snapshot(stm_snapshot_header, iobuf&&) override;
    stm_snapshot take_snapshot() override;

private:
    struct seq_entry {
        model::producer_identity pid;
        int32_t seq;
        model::timestamp::type last_write_timestamp;
    };

    struct seq_snapshot {
        model::offset offset;
        std::vector<seq_entry> entries;
    };

    /*
     * Usually start() acts as a barrier and we don't call any methods on the
     * object before start returns control flow.
     *
     * With snapshot-enabled stm we have the following workflow around
     * partition.h/.cc:
     *
     * 1. create consensus
     * 2. create partition
     * 3. pass consensus to partition
     * 4. inside partition:
     *    - create stm and pass consensus to constructor
     *    - pass stm to consensus via stm_manager
     *    - start consensus
     *    - start stm
     *
     * We can't start stm before starting consensus but once consensus has
     * started it may get a chance to invoke make_snapshot or
     * ensure_snapshot_exists on stm before it's started.
     *
     * `wait_for_snapshot_hydrated` inside those methods protects from this
     * scenario.
     */
    void compact_snapshot();

    absl::flat_hash_map<model::producer_identity, seq_entry> _seq_table;
    model::timestamp _oldest_session;
    ss::future<> apply(model::record_batch b) override;
};

} // namespace cluster
