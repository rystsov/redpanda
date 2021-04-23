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
    
private:
    [[maybe_unused]] ss::sharded<cluster::metadata_cache>& _metadata_cache;
    [[maybe_unused]] ss::sharded<rpc::connection_cache>& _connection_cache;
    [[maybe_unused]] ss::sharded<partition_leaders_table>& _leaders;
    [[maybe_unused]] std::unique_ptr<cluster::controller>& _controller;
    [[maybe_unused]] ss::sharded<kafka::coordinator_ntp_mapper>& _coordinator_mapper;
    [[maybe_unused]] ss::sharded<kafka::group_router>& _group_router;
    [[maybe_unused]] int16_t _metadata_dissemination_retries;
    [[maybe_unused]] std::chrono::milliseconds _metadata_dissemination_retry_delay_ms;
};
} // namespace cluster
