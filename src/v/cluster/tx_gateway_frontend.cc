// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_gateway_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"

#include "cluster/logger.h"

namespace cluster {

tx_gateway_frontend::tx_gateway_frontend(
    ss::smp_service_group ssg,
    ss::sharded<cluster::partition_manager>& partition_manager,
    ss::sharded<cluster::shard_table>& shard_table,
    ss::sharded<cluster::metadata_cache>& metadata_cache,
    ss::sharded<rpc::connection_cache>& connection_cache,
    ss::sharded<partition_leaders_table>& leaders,
    std::unique_ptr<cluster::controller>& controller)
    : _ssg(ssg)
    , _partition_manager(partition_manager)
    , _shard_table(shard_table)
    , _metadata_cache(metadata_cache)
    , _connection_cache(connection_cache)
    , _leaders(leaders)
    , _controller(controller) {}

ss::future<std::optional<model::node_id>>
tx_gateway_frontend::get_tx_broker([[maybe_unused]] ss::sstring key) {
    auto nt = model::topic_namespace(model::kafka_internal_namespace, model::kafka_tx_topic);
    
    auto has_topic = ss::make_ready_future<bool>(true);

    if (!_metadata_cache.local().contains(nt, model::partition_id(0))) {
        has_topic = try_create_tx_topic();
    }

    auto timeout = ss::lowres_clock::now() + config::shard_local_cfg().wait_for_leader_timeout_ms();

    return has_topic.then([this, nt, timeout](bool does_topic_exist) {
        if (!does_topic_exist) {
            return ss::make_ready_future<std::optional<model::node_id>>(std::nullopt);
        }

        auto md = _metadata_cache.local().get_topic_metadata(nt);
        if (!md) {
            return ss::make_ready_future<std::optional<model::node_id>>(std::nullopt);
        }
        auto ntp = model::ntp(nt.ns, nt.tp, model::partition_id{0});
        return _metadata_cache.local()
          .get_leader(ntp, timeout)
          .then([]([[maybe_unused]] model::node_id leader) { 
            return std::optional<model::node_id>(leader);
          })
          .handle_exception([]([[maybe_unused]] std::exception_ptr e) {
              return ss::make_ready_future<std::optional<model::node_id>>(std::nullopt);
          });
    });
}

ss::future<bool>
tx_gateway_frontend::try_create_tx_topic() {
    cluster::topic_configuration topic{
      model::kafka_internal_namespace,
      model::kafka_tx_topic,
      1,
      config::shard_local_cfg().default_topic_replication()};

    topic.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;

    return _controller->get_topics_frontend()
      .local()
      .autocreate_topics(
        {std::move(topic)}, config::shard_local_cfg().create_topic_timeout_ms())
      .then([](std::vector<cluster::topic_result> res) {
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec != cluster::errc::success) {
              return false;
          }
          return true;
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(clusterlog.warn, "cant create allocate id stm topic {}", e);
          return false;
      });
}

} // namespace cluster
