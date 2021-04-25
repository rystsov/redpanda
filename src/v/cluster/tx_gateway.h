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
#include "cluster/fwd.h"
#include "cluster/tx_gateway_service.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class tx_gateway final : public tx_gateway_service {
public:
    tx_gateway(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<cluster::tx_gateway_frontend>&,
      ss::sharded<cluster::rm_group_frontend>&,
      ss::sharded<cluster::rm_partition_frontend>&);

    virtual ss::future<init_tm_tx_reply>
    init_tm_tx(init_tm_tx_request&&, rpc::streaming_context&) final;
    
    virtual ss::future<begin_tx_reply>
    begin_tx(begin_tx_request&&, rpc::streaming_context&) final;

    virtual ss::future<prepare_tx_reply>
    prepare_tx(prepare_tx_request&&, rpc::streaming_context&) final;

    virtual ss::future<commit_tx_reply>
    commit_tx(commit_tx_request&&, rpc::streaming_context&) final;

    virtual ss::future<abort_tx_reply>
    abort_tx(abort_tx_request&&, rpc::streaming_context&) final;

    virtual ss::future<begin_group_tx_reply>
    begin_group_tx(begin_group_tx_request&&, rpc::streaming_context&) final;

    virtual ss::future<prepare_group_tx_reply>
    prepare_group_tx(prepare_group_tx_request&&,rpc::streaming_context&) final;

    virtual ss::future<commit_group_tx_reply>
    commit_group_tx(commit_group_tx_request&&, rpc::streaming_context&) final;

    virtual ss::future<abort_group_tx_reply>
    abort_group_tx(abort_group_tx_request&&, rpc::streaming_context&) final;

private:
    [[maybe_unused]] ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    [[maybe_unused]] ss::sharded<cluster::rm_group_frontend>& _rm_group_frontend;
    [[maybe_unused]] ss::sharded<cluster::rm_partition_frontend>& _rm_partition_frontend;
};
}
