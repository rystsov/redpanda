// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/add_offsets_to_txn.h"
#include "kafka/server/handlers/add_partitions_to_txn.h"

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
ss::future<response_ptr>
add_offsets_to_txn_handler::handle(request_context ctx, ss::smp_service_group) {
     return ss::do_with(std::move(ctx), [](request_context& ctx) {
        add_offsets_to_txn_request request;
        request.decode(ctx.reader(), ctx.header().version);
        
        add_partitions_to_txn_request fake;
        fake.data.transactional_id = request.data.transactional_id;
        fake.data.producer_id = request.data.producer_id;
        fake.data.producer_epoch = request.data.producer_epoch;

        auto f = ctx.tx_gateway_frontend().add_partition_to_tx(fake.data, config::shard_local_cfg().create_topic_timeout_ms());

        return f.then([&ctx]([[maybe_unused]] add_partitions_to_txn_response_data data) {
            add_offsets_to_txn_response res;
            res.data.error_code = error_code::none;
            return ctx.respond(std::move(res));
        });
     });
}

} // namespace kafka
