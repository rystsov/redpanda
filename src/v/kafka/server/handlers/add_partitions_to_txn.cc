// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/add_partitions_to_txn.h"

#include "cluster/topics_frontend.h"
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
ss::future<response_ptr> add_partitions_to_txn_handler::handle(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
     return ss::do_with(std::move(ctx), [](request_context& ctx) {
        add_partitions_to_txn_request request;
        request.decode(ctx.reader(), ctx.header().version);

        add_partitions_to_txn_response response;
        for (auto& req_topic : request.data.topics) {
            add_partitions_to_txn_topic_result res_topic;
            res_topic.name = req_topic.name;
            for (int32_t req_partition : req_topic.partitions) {
                add_partitions_to_txn_partition_result res_partition;
                res_partition.partition_index = req_partition;
                res_partition.error_code = kafka::error_code::none;
                res_topic.results.push_back(res_partition);
            }
            response.data.results.push_back(res_topic);
        }

        return ctx.respond(std::move(response));
     });
}

} // namespace kafka
