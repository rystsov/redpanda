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

#include "pandaproxy/context.h"
#include "pandaproxy/json/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>

namespace pandaproxy {

/// \brief wrapper around ss::httpd.
///
/// Server is the basis of middleware component to allow
/// strategies to be composed around request/response,
/// e.g., logging, serialisation, metrics, rate-limiting.
class server {
public:
    struct context_t {
        std::vector<unresolved_address> advertised_listeners;
        ss::semaphore mem_sem;
        ss::abort_source as;
        ss::smp_service_group smp_sg;
        ss::sharded<kafka::client::client>& client;
        const configuration& config;
    };

    struct request_t {
        std::unique_ptr<ss::httpd::request> req;
        context_t& ctx;
        // will contain other extensions passed to user specific handler.
    };

    struct reply_t {
        std::unique_ptr<ss::httpd::reply> rep;
        json::serialization_format mime_type;
        // will contain other extensions passed to user specific handler.
    };

    using function_handler
      = ss::noncopyable_function<ss::future<reply_t>(request_t, reply_t)>;

    struct route_t {
        ss::path_description path_desc;
        function_handler handler;
    };

    struct routes_t {
        ss::sstring api;
        std::vector<route_t> routes;
    };

    server() = delete;
    ~server() = default;
    server(const server&) = delete;
    server(server&&) = default;
    server& operator=(const server&) = delete;
    server& operator=(server&&) = delete;

    server(
      const ss::sstring& server_name,
      ss::api_registry_builder20&& api20,
      pandaproxy::context_t ctx);

    void route(route_t route);
    void routes(routes_t&& routes);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::httpd::http_server _server;
    ss::gate _pending_reqs;
    ss::api_registry_builder20 _api20;
    bool _has_routes;
    context_t _ctx;
};

} // namespace pandaproxy
