// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/end_txn.h"

#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

template<>
ss::future<response_ptr> end_txn_handler::handle(request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        end_txn_request request;
        request.decode(ctx.reader(), ctx.header().version);
        return ctx.tx_gateway_frontend().end_txn(request.data, config::shard_local_cfg().create_topic_timeout_ms()).then([&ctx](end_txn_response_data data) {
            end_txn_response response;
            response.data = data;
            return ctx.respond(std::move(response));
        });
    });
}

} // namespace kafka
