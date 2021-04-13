// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_gateway.h"

#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "cluster/tx_gateway_frontend.h"
#include <seastar/core/coroutine.hh>

#include <seastar/core/sharded.hh>

namespace cluster {

tx_gateway::tx_gateway(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend)
  : tx_gateway_service(sg, ssg)
  , _tx_gateway_frontend(tx_gateway_frontend) {}

ss::future<init_tm_tx_reply>
tx_gateway::init_tm_tx(init_tm_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().do_init_tm_tx(request.tx_id, request.timeout);
}

ss::future<begin_tx_reply>
tx_gateway::begin_tx(begin_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().do_begin_tx(request.ntp, request.pid);
}

ss::future<prepare_tx_reply>
tx_gateway::prepare_tx(prepare_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().do_prepare_tx(request.ntp, request.etag, request.tm, request.pid, request.tx_seq, request.timeout);
}

ss::future<commit_tx_reply>
tx_gateway::commit_tx(commit_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().do_commit_tx(request.ntp, request.pid, request.tx_seq, request.timeout);
}

ss::future<abort_tx_reply>
tx_gateway::abort_tx(abort_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().do_abort_tx(request.ntp, request.pid, request.timeout);
}

ss::future<begin_group_tx_reply>
tx_gateway::begin_group_tx(begin_group_tx_request&& request, [[maybe_unused]] rpc::streaming_context&) {
    //vlog(clusterlog.info, "SHAI(10): gateway:begin_group_tx");
    auto reply = co_await _tx_gateway_frontend.local().do_begin_group_tx(request.group_id, request.pid, request.timeout);
    //vlog(clusterlog.info, "SHAI(11): gateway:begin_group_tx {}", reply.ec);
    co_return reply;
};

ss::future<prepare_group_tx_reply>
tx_gateway::prepare_group_tx(prepare_group_tx_request&& request, [[maybe_unused]] rpc::streaming_context&) {
    return _tx_gateway_frontend.local().do_prepare_group_tx(request.group_id, request.etag, request.pid, request.tx_seq, request.timeout);
};

ss::future<commit_group_tx_reply>
tx_gateway::commit_group_tx(commit_group_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().do_commit_group_tx(request.group_id, request.pid, request.tx_seq, request.timeout);
};

ss::future<abort_group_tx_reply>
tx_gateway::abort_group_tx(abort_group_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().do_abort_group_tx(request.group_id, request.pid, request.timeout);
}

} // namespace cluster
