// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/init_producer_id.h"

#include "cluster/id_allocator_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
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
ss::future<response_ptr> init_producer_id_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        vlog(klog.trace, "processing init_producer_id");

        init_producer_id_request request;
        request.decode(ctx.reader(), ctx.header().version);

        // if (!ctx.authorized(
        //           security::acl_operation::write,
        //           transactional_id(*request.data.transactional_id))) {
        //         init_producer_id_response reply;
        //         reply.data.error_code
        //           = error_code::transactional_id_authorization_failed;
        //         return ctx.respond(reply);
        //     }
        // } else if (!ctx.authorized(
        //              security::acl_operation::idempotent_write,
        //              security::default_cluster_name)) {
        //     init_producer_id_response reply;
        //     reply.data.error_code = error_code::cluster_authorization_failed;
        //     return ctx.respond(reply);
        // }

        if (request.data.transactional_id) {
          return ctx.tx_gateway_frontend()
            .init_tm_tx(request.data.transactional_id.value(), config::shard_local_cfg().create_topic_timeout_ms())
            .then([&ctx](cluster::init_tm_tx_reply r) {
                init_producer_id_response reply;

                if (r.ec == cluster::tx_errc::none) {
                    reply.data.producer_id = kafka::producer_id(r.pid.id);
                    reply.data.producer_epoch = r.pid.epoch;
                    vlog(
                      klog.info,
                      "allocated pid {} with epoch {} via tx_gateway",
                      reply.data.producer_id,
                      reply.data.producer_epoch);
                } else {
                    vlog(klog.warn, "failed to allocate pid");
                    reply.data.error_code = error_code::broker_not_available;
                }

                return ctx.respond(std::move(reply));
            });
        } else {
          return ctx.id_allocator_frontend()
            .allocate_id(config::shard_local_cfg().create_topic_timeout_ms())
            .then([&ctx](cluster::allocate_id_reply r) {
                init_producer_id_response reply;

                if (r.ec == cluster::errc::success) {
                    reply.data.producer_id = kafka::producer_id(r.id);
                    reply.data.producer_epoch = 0;
                    vlog(
                      klog.trace,
                      "allocated pid {} with epoch {}",
                      reply.data.producer_id,
                      reply.data.producer_epoch);
                } else {
                    vlog(klog.warn, "failed to allocate pid");
                    reply.data.error_code = error_code::broker_not_available;
                }

                return ctx.respond(std::move(reply));
            });
        }
    });
}

} // namespace kafka
