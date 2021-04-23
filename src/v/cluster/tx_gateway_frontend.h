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
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_router.h"
#include "kafka/protocol/errors.h"
#include "model/metadata.h"
#include "cluster/metadata_cache.h"
#include "seastarx.h"
#include "cluster/tx_gateway.h"
#include "kafka/protocol/add_partitions_to_txn.h"
#include "kafka/protocol/add_offsets_to_txn.h"
#include "kafka/protocol/end_txn.h"
#include "cluster/tm_stm.h"

namespace cluster {

struct tx_coordinator_address {
    kafka::error_code error;
    model::node_id node;
    ss::sstring host;
    int32_t port;
};

class tx_gateway_frontend final : public ss::peering_sharded_service<tx_gateway_frontend> {
public:
    tx_gateway_frontend(
        ss::smp_service_group,
        ss::sharded<cluster::partition_manager>&,
        ss::sharded<cluster::shard_table>&,
        ss::sharded<cluster::metadata_cache>&,
        ss::sharded<rpc::connection_cache>&,
        ss::sharded<partition_leaders_table>&,
        std::unique_ptr<cluster::controller>&,
        ss::sharded<cluster::id_allocator_frontend>&,
        ss::sharded<cluster::rm_group_frontend>&,
        ss::sharded<cluster::rm_partition_frontend>&,
        ss::sharded<kafka::coordinator_ntp_mapper>&,
        ss::sharded<kafka::group_router>&);

    ss::future<std::optional<model::node_id>> get_tx_broker(ss::sstring);
    ss::future<init_tm_tx_reply> init_tm_tx(kafka::transactional_id, model::timeout_clock::duration);
    ss::future<cluster::init_tm_tx_reply> init_tm_tx_locally(kafka::transactional_id, model::timeout_clock::duration);
    ss::future<kafka::add_partitions_to_txn_response_data> add_partition_to_tx(kafka::add_partitions_to_txn_request_data, model::timeout_clock::duration);
    ss::future<kafka::add_offsets_to_txn_response_data> add_offsets_to_tx(kafka::add_offsets_to_txn_request_data, model::timeout_clock::duration);
    ss::future<kafka::end_txn_response_data> end_txn(kafka::end_txn_request_data, model::timeout_clock::duration);

private:
    ss::smp_service_group _ssg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_leaders_table>& _leaders;
    std::unique_ptr<cluster::controller>& _controller;
    ss::sharded<cluster::id_allocator_frontend>& _id_allocator_frontend;
    ss::sharded<cluster::rm_group_frontend>& _rm_group_frontend;
    ss::sharded<cluster::rm_partition_frontend>& _rm_partition_frontend;
    ss::sharded<kafka::coordinator_ntp_mapper>& _coordinator_mapper;
    ss::sharded<kafka::group_router>& _group_router;
    int16_t _metadata_dissemination_retries;
    std::chrono::milliseconds _metadata_dissemination_retry_delay_ms;

    ss::future<bool> try_create_tx_topic();

    ss::future<checked<tm_transaction, tx_errc>> get_tx(ss::shared_ptr<tm_stm>&, model::producer_identity, kafka::transactional_id, model::timeout_clock::duration);

    ss::future<cluster::init_tm_tx_reply> dispatch_init_tm_tx(model::node_id, kafka::transactional_id, model::timeout_clock::duration);
    ss::future<cluster::init_tm_tx_reply> do_init_tm_tx(ss::shard_id, kafka::transactional_id, model::timeout_clock::duration);
    ss::future<cluster::init_tm_tx_reply> do_init_tm_tx(kafka::transactional_id, model::timeout_clock::duration);

    ss::future<checked<cluster::tm_transaction, tx_errc>> abort_tm_tx(ss::shared_ptr<cluster::tm_stm>&, cluster::tm_transaction, model::timeout_clock::duration, ss::lw_shared_ptr<ss::promise<tx_errc>>);
    ss::future<checked<cluster::tm_transaction, tx_errc>> do_abort_tm_tx(ss::shared_ptr<cluster::tm_stm>&, cluster::tm_transaction, model::timeout_clock::duration, ss::lw_shared_ptr<ss::promise<tx_errc>>);
    ss::future<checked<cluster::tm_transaction, tx_errc>> commit_tm_tx(ss::shared_ptr<cluster::tm_stm>&, cluster::tm_transaction, model::timeout_clock::duration, ss::lw_shared_ptr<ss::promise<tx_errc>>);
    ss::future<checked<cluster::tm_transaction, tx_errc>> do_commit_tm_tx(ss::shared_ptr<cluster::tm_stm>&, cluster::tm_transaction, model::timeout_clock::duration, ss::lw_shared_ptr<ss::promise<tx_errc>>);
    ss::future<checked<tm_transaction, tx_errc>> recommit_tm_tx(ss::shared_ptr<tm_stm>&, tm_transaction, model::timeout_clock::duration);
    ss::future<checked<tm_transaction, tx_errc>> reabort_tm_tx(ss::shared_ptr<tm_stm>&, tm_transaction, model::timeout_clock::duration);

    ss::future<kafka::add_partitions_to_txn_response_data> do_add_partition_to_tx(ss::shared_ptr<tm_stm>&, kafka::add_partitions_to_txn_request_data, model::timeout_clock::duration);
    ss::future<kafka::add_offsets_to_txn_response_data> do_add_offsets_to_tx(ss::shared_ptr<tm_stm>&, kafka::add_offsets_to_txn_request_data, model::timeout_clock::duration);
    friend tx_gateway;
};
} // namespace cluster
