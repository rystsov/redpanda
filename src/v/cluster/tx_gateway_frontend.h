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
#include "kafka/types.h"
#include "kafka/protocol/errors.h"
#include "model/metadata.h"
#include "cluster/metadata_cache.h"
#include "seastarx.h"

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
        ss::sharded<cluster::metadata_cache>&,
        std::unique_ptr<cluster::controller>&);

    ss::future<std::optional<model::node_id>> get_tx_broker(ss::sstring);

private:
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    std::unique_ptr<cluster::controller>& _controller;

    ss::future<bool> try_create_tx_topic();
};
} // namespace cluster
