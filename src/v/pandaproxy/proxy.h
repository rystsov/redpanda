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

#include "kafka/client/client.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/server.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/socket_defs.hh>

#include <vector>

namespace pandaproxy {

class proxy {
public:
    proxy(
      const YAML::Node& config,
      ss::smp_service_group smp_sg,
      ss::sharded<kafka::client::client>& client);

    ss::future<> start();
    ss::future<> stop();

    configuration& config();
    kafka::client::configuration& client_config();

private:
    configuration _config;
    ss::smp_service_group _smp_sg;
    ss::sharded<kafka::client::client>& _client;
    context_t _ctx;
    server _server;
};

} // namespace pandaproxy
