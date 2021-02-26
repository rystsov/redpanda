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
#include "cluster/types.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

struct tx_snapshot_header {
    static constexpr const int8_t supported_version = 0;

    int8_t version{tx_snapshot_header::supported_version};
    int32_t snapshot_size{0};

    static constexpr const size_t ondisk_size = sizeof(version)
                                                + sizeof(snapshot_size);
};

class tx_stm final
  : public raft::state_machine
  , public storage::snapshotable_stm {
public:
    static constexpr model::control_record_version prepare_control_record_version{0};
    
    explicit tx_stm(ss::logger&, raft::consensus*, config::configuration&);

    ss::future<> start() final;

    ss::future<> ensure_snapshot_exists(model::offset) final;
    ss::future<> make_snapshot() final;
    ss::future<> catchup();

    ss::future<tx_errc> abort_tx(model::producer_identity, model::timeout_clock::time_point);
    ss::future<tx_errc> prepare_tx(model::term_id, model::partition_id, model::producer_identity, model::timeout_clock::time_point);
    ss::future<tx_errc> commit_tx(model::producer_identity, model::timeout_clock::time_point);
    std::optional<model::term_id> begin_tx(model::producer_identity);

    ss::future<checked<raft::replicate_result, kafka::error_code>> replicate(
      model::batch_identity,
      model::record_batch_reader&&,
      raft::replicate_options);

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

    ss::future<> apply(model::record_batch b) override;

    model::offset _last_snapshot_offset;
    mutex _op_lock;
    
    // track ongoing pids->[offsets]
    // X: ordered set of pids offset
    // get min of X
    // on commit(pid) evict all pid's offset
    // on abort(pid):
    //   add [min,max] to aborted 
    // todo replay

    ////////////////////////////////////

    absl::flat_hash_map<model::producer_identity, model::term_id> _expected;

    // {expected}:  map  pid -> term
    // {estimated}: list [(pid,offset)]
    // {ongoing}:   set  offset
    //              map  pid->offset
    // {prepared}:  set  pid
    // {aborted}:   list [($pid, first, last)]

    /*
when begin(pid) comes:
  [must be in-sync]
  if $pid in {expected} set => error
  put ($pid, term) to {expected} set

when a tx record(pid) comes:
  if $pid isn't in {expected} set:
    error!
  if $pid is in {ongoing} or {estimated}:
    replicate with expected term!
  else:
    use insync offset and put ($pid, offset) to {estimated}
    replicate with expected term!
    if replication hasn't failed:
      remove ($pid, *) from {estimated}
      put correct ($pid, offset) to {ongoing}

when prepare(pid, term) comes:
  if $pid in {prepared} => reply ok
  if $pid isn't in {expected} => error
  replicate with $term!
  if replication hasn't failed:
    remove $pid from {expected}
    put $pid to {prepared}

when commit(pid) comes:
  [must be in-sync]
  if $pid isn't in {expected} and isn't in {prepared}:
    // was already commited in the past
    reply ok
  if $pid isn't in {prepared}:
    error
  replicate with insync term!
  if replication hasn't failed:
    for ($pid, first) in {ongoing} or {estimated}:
      evict!
    remove $pid from {prepared}
    remove $pid from {expected}

when abort(pid) comes:
  [must be in-sync]
  if $pid isn't in {expected} and isn't in {prepared}:
    // was already aborted in the past
    reply ok
  replicate with insync term!
  if replication hasn't failed:
    for ($pid, first) in {ongoing} or {estimated}:
      evict!
      put ($pid, first, offset) to {aborted}
      remove $pid from {prepared}
      remove $pid from {expected}

    */
    
    ss::shared_promise<> _resolved_when_snapshot_hydrated;
    bool _is_catching_up{false};
    model::term_id _insync_term{-1};
    model::offset _insync_offset{-1};
    raft::consensus* _c;
    storage::snapshot_manager _snapshot_mgr;
    ss::logger& _log;
};

} // namespace raft
