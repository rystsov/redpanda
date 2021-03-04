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
#include "cluster/persisted_stm.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/btree_set.h>

namespace cluster {

class tx_stm final
  : public persisted_stm {
public:
    static constexpr const int8_t supported_version = 0;
    using producer_id = named_type<int64_t, struct producer_identity_id>;
    using producer_epoch = named_type<int64_t, struct producer_identity_epoch>;

    struct tx_range {
        model::producer_identity pid;
        model::offset first;
        model::offset last;
    };

    struct tx_snapshot {
        std::vector<tx_range> ongoing;
        std::vector<model::producer_identity> prepared;
        std::vector<tx_range> aborted;
        model::offset offset;
    };
    
    static constexpr model::control_record_version prepare_control_record_version{0};
    
    explicit tx_stm(ss::logger&, raft::consensus*, config::configuration&);

    ss::future<tx_errc> abort_tx(model::producer_identity, model::timeout_clock::time_point);
    ss::future<tx_errc> prepare_tx(model::term_id, model::partition_id, model::producer_identity, model::timeout_clock::time_point);
    ss::future<tx_errc> commit_tx(model::producer_identity, model::timeout_clock::time_point);
    ss::future<std::optional<model::term_id>> begin_tx(model::producer_identity);

    std::optional<model::offset> tx_lso();
    std::vector<tx_stm::tx_range> aborted_transactions(model::offset, model::offset);

    ss::future<checked<raft::replicate_result, kafka::error_code>> replicate(
      model::batch_identity,
      model::record_batch_reader&&,
      raft::replicate_options);

protected:
    void load_snapshot(stm_snapshot_header, iobuf&&) override;
    stm_snapshot take_snapshot() override;

private:
    void compact_snapshot();

    ss::future<> apply(model::record_batch b) override;
    
    // track ongoing pids->[offsets]
    // X: ordered set of pids offset
    // get min of X
    // on commit(pid) evict all pid's offset
    // on abort(pid):
    //   add [min,max] to aborted 
    // todo replay

    ////////////////////////////////////

    absl::flat_hash_map<producer_id, model::term_id> _expected;
    absl::flat_hash_map<model::producer_identity, model::offset> _estimated;
    absl::flat_hash_map<model::producer_identity, bool> _has_prepare_applied;
    absl::flat_hash_map<model::producer_identity, bool> _has_commit_applied;
    absl::flat_hash_map<producer_id, producer_epoch> _fence_pid_epoch;
    
    absl::flat_hash_map<model::producer_identity, tx_range> _ongoing_map;
    absl::btree_set<model::offset> _ongoing_set;
    absl::btree_set<model::producer_identity> _prepared;
    std::vector<tx_range> _aborted;


    // {expected}:  map  pid -> term
    // {estimated}: list [(pid,offset)]
    // {ongoing}:   set  offset
    //              map  pid->offset
    // {prepared}:  map  pid -> term
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
  if $pid in {expected}:
    error
  if $pid isn't in {prepared}:
    // was already commited in the past
    reply ok
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
};

} // namespace raft
