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
#include "cluster/controller.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway.h"
#include "cluster/types.h"
#include "kafka/types.h"
#include "kafka/protocol/errors.h"
#include "model/metadata.h"
#include "cluster/metadata_cache.h"
#include "seastarx.h"
#include "kafka/protocol/add_partitions_to_txn.h"
#include "kafka/protocol/add_offsets_to_txn.h"
#include "kafka/protocol/end_txn.h"
#include "cluster/tm_stm.h"

namespace cluster {

class rm_group_frontend {
public:
    rm_group_frontend(
        ss::sharded<cluster::metadata_cache>&,
        ss::sharded<rpc::connection_cache>&,
        ss::sharded<partition_leaders_table>&,
        std::unique_ptr<cluster::controller>&,
        ss::sharded<kafka::coordinator_ntp_mapper>&,
        ss::sharded<kafka::group_router>&);
    
    ss::future<begin_group_tx_reply> begin_group_tx(kafka::group_id, model::producer_identity, model::timeout_clock::duration);
    ss::future<prepare_group_tx_reply> prepare_group_tx(kafka::group_id, model::term_id, model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<commit_group_tx_reply> commit_group_tx(kafka::group_id, model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<abort_group_tx_reply> abort_group_tx(kafka::group_id, model::producer_identity, model::timeout_clock::duration);

private:
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_leaders_table>& _leaders;
    std::unique_ptr<cluster::controller>& _controller;
    ss::sharded<kafka::coordinator_ntp_mapper>& _coordinator_mapper;
    ss::sharded<kafka::group_router>& _group_router;
    int16_t _metadata_dissemination_retries;
    std::chrono::milliseconds _metadata_dissemination_retry_delay_ms;

    ss::future<bool> try_create_consumer_group_topic();

    ss::future<begin_group_tx_reply> dispatch_begin_group_tx(model::node_id, kafka::group_id, model::producer_identity, model::timeout_clock::duration);
    ss::future<begin_group_tx_reply> do_begin_group_tx(kafka::group_id, model::producer_identity, model::timeout_clock::duration);
    ss::future<prepare_group_tx_reply> dispatch_prepare_group_tx(model::node_id, kafka::group_id, model::term_id, model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<prepare_group_tx_reply> do_prepare_group_tx(kafka::group_id, model::term_id, model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<commit_group_tx_reply> dispatch_commit_group_tx(model::node_id, kafka::group_id, model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<commit_group_tx_reply> do_commit_group_tx(kafka::group_id, model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<abort_group_tx_reply> dispatch_abort_group_tx(model::node_id, kafka::group_id, model::producer_identity, model::timeout_clock::duration);
    ss::future<abort_group_tx_reply> do_abort_group_tx(kafka::group_id, model::producer_identity, model::timeout_clock::duration);

    friend tx_gateway;
};
} // namespace cluster