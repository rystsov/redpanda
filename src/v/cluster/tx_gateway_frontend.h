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
#include "cluster/types.h"
#include "kafka/types.h"
#include "kafka/protocol/errors.h"
#include "model/metadata.h"
#include "cluster/metadata_cache.h"
#include "seastarx.h"
#include "cluster/tx_gateway.h"

namespace cluster {

struct tx_coordinator_address {
    kafka::error_code error;
    model::node_id node;
    ss::sstring host;
    int32_t port;
};

class tx_gateway_frontend {
public:
    tx_gateway_frontend(
        ss::smp_service_group,
        ss::sharded<cluster::partition_manager>&,
        ss::sharded<cluster::shard_table>&,
        ss::sharded<cluster::metadata_cache>&,
        ss::sharded<rpc::connection_cache>&,
        ss::sharded<partition_leaders_table>&,
        std::unique_ptr<cluster::controller>&);

    ss::future<std::optional<model::node_id>> get_tx_broker(ss::sstring);
    ss::future<abort_tx_reply> abort_tx(model::ntp, model::producer_identity, model::timeout_clock::duration);
    ss::future<prepare_tx_reply> prepare_tx(model::ntp, model::term_id, model::producer_identity, model::timeout_clock::duration);
    ss::future<commit_tx_reply> commit_tx(model::ntp, model::producer_identity, model::timeout_clock::duration);

private:
    [[maybe_unused]] ss::smp_service_group _ssg;
    [[maybe_unused]] ss::sharded<cluster::partition_manager>& _partition_manager;
    [[maybe_unused]] ss::sharded<cluster::shard_table>& _shard_table;
    [[maybe_unused]] ss::sharded<cluster::metadata_cache>& _metadata_cache;
    [[maybe_unused]] ss::sharded<rpc::connection_cache>& _connection_cache;
    [[maybe_unused]] ss::sharded<partition_leaders_table>& _leaders;
    [[maybe_unused]] std::unique_ptr<cluster::controller>& _controller;

    ss::future<bool> try_create_tx_topic();
    ss::future<abort_tx_reply> dispatch_abort_tx(model::node_id, model::ntp, model::producer_identity, model::timeout_clock::duration);
    ss::future<abort_tx_reply> do_abort_tx(model::ntp, model::producer_identity, model::timeout_clock::duration);
    ss::future<prepare_tx_reply> dispatch_prepare_tx(model::node_id, model::ntp, model::term_id, model::producer_identity, model::timeout_clock::duration);
    ss::future<prepare_tx_reply> do_prepare_tx(model::ntp, model::term_id, model::producer_identity, model::timeout_clock::duration);
    ss::future<commit_tx_reply> dispatch_commit_tx(model::node_id, model::ntp, model::producer_identity, model::timeout_clock::duration);
    ss::future<commit_tx_reply> do_commit_tx(model::ntp, model::producer_identity, model::timeout_clock::duration);

    friend tx_gateway;
};
} // namespace cluster