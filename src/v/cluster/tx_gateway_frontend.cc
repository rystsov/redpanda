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
#include "cluster/tx_gateway_service.h"

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

ss::future<prepare_tx_reply>
tx_gateway_frontend::prepare_tx(model::ntp ntp, model::term_id etag, model::producer_identity pid, model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        return ss::make_ready_future<prepare_tx_reply>(prepare_tx_reply {
            .ec = tx_errc::partition_not_exists
        });
    }

    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        vlog(clusterlog.warn, "can't find a leader for {}", ntp);
        return ss::make_ready_future<prepare_tx_reply>(prepare_tx_reply {
            .ec = tx_errc::leader_not_found
        });
    }

    auto _self = _controller->self();

    if (leader == _self) {
        return do_prepare_tx(ntp, etag, pid, timeout);
    }

    vlog(clusterlog.trace, "dispatching prepare tx to {} from {}", leader, _self);

    return dispatch_prepare_tx(leader.value(), ntp, etag, pid, timeout);
}

ss::future<prepare_tx_reply>
tx_gateway_frontend::dispatch_prepare_tx(model::node_id leader, model::ntp ntp, model::term_id etag, model::producer_identity pid, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [ntp, etag, pid, timeout](tx_gateway_client_protocol cp) {
            return cp.prepare_tx(
              prepare_tx_request{
                  .ntp = ntp,
                  .etag = etag,
                  .pid = pid,
                  .timeout = timeout
                },
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<prepare_tx_reply>)
      .then([](result<prepare_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote prepare tx",
                r.error());
              return prepare_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<prepare_tx_reply>
tx_gateway_frontend::do_prepare_tx(model::ntp ntp, model::term_id etag, model::producer_identity pid, model::timeout_clock::duration timeout) {
    auto shard = _shard_table.local().shard_for(ntp);

    if (shard == std::nullopt) {
        vlog(clusterlog.warn, "can't find a shard for {}", ntp);
        return ss::make_ready_future<prepare_tx_reply>(prepare_tx_reply {
            .ec = tx_errc::shard_not_found
        });
    }

    return _partition_manager.invoke_on(
      *shard, _ssg, [ntp, etag, pid, timeout](cluster::partition_manager& mgr) mutable {
          vlog(clusterlog.warn, "timeout {}", timeout);
          auto partition = mgr.get(ntp);
          if (!partition) {
              vlog(clusterlog.warn, "can't get partition by {} ntp", ntp);
              return ss::make_ready_future<prepare_tx_reply>(prepare_tx_reply {
                  .ec = tx_errc::partition_not_found
              });
          }

          auto& stm = partition->tx_stm();

          if (!stm) {
              vlog(clusterlog.warn, "can't get tx stm of the {}' partition", ntp);
              return ss::make_ready_future<prepare_tx_reply>(prepare_tx_reply{
                  .ec = tx_errc::stm_not_found
              });
          }

          // ok, tx not found, timeout
          return stm->prepare_tx(etag, pid, model::timeout_clock::now() + timeout).then([](){
              return prepare_tx_reply();
          });
      });
}

ss::future<abort_tx_reply>
tx_gateway_frontend::abort_tx(model::ntp ntp, model::producer_identity pid, model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        return ss::make_ready_future<abort_tx_reply>(abort_tx_reply {
            .ec = tx_errc::partition_not_exists
        });
    }

    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        vlog(clusterlog.warn, "can't find a leader for {}", ntp);
        return ss::make_ready_future<abort_tx_reply>(abort_tx_reply {
            .ec = tx_errc::leader_not_found
        });
    }

    auto _self = _controller->self();

    if (leader == _self) {
        return do_abort_tx(ntp, pid, timeout);
    }

    vlog(clusterlog.trace, "dispatching abort tx to {} from {}", leader, _self);

    return dispatch_abort_tx(leader.value(), ntp, pid, timeout);
}

ss::future<abort_tx_reply>
tx_gateway_frontend::dispatch_abort_tx(model::node_id leader, model::ntp ntp, model::producer_identity pid, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [ntp, pid, timeout](tx_gateway_client_protocol cp) {
            return cp.abort_tx(
              abort_tx_request{
                  .ntp = ntp,
                  .pid = pid,
                  .timeout = timeout
                },
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<abort_tx_reply>)
      .then([](result<abort_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote abort tx",
                r.error());
              return abort_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<abort_tx_reply>
tx_gateway_frontend::do_abort_tx(model::ntp ntp, model::producer_identity pid, model::timeout_clock::duration timeout) {
    
    auto shard = _shard_table.local().shard_for(ntp);

    if (shard == std::nullopt) {
        vlog(clusterlog.warn, "can't find a shard for {}", ntp);
        return ss::make_ready_future<abort_tx_reply>(abort_tx_reply {
            .ec = tx_errc::shard_not_found
        });
    }

    return _partition_manager.invoke_on(
      *shard, _ssg, [pid, ntp, timeout](cluster::partition_manager& mgr) mutable {
          vlog(clusterlog.warn, "timeout {}", timeout);
          auto partition = mgr.get(ntp);
          if (!partition) {
              vlog(clusterlog.warn, "can't get partition by {} ntp", ntp);
              return ss::make_ready_future<abort_tx_reply>(abort_tx_reply {
                  .ec = tx_errc::partition_not_found
              });
          }

          auto& stm = partition->tx_stm();

          if (!stm) {
              vlog(clusterlog.warn, "can't get tx stm of the {}' partition", ntp);
              return ss::make_ready_future<abort_tx_reply>(abort_tx_reply{
                  .ec = tx_errc::stm_not_found
              });
          }

          // ok, tx not found, timeout
          return stm->abort_tx(pid, model::timeout_clock::now() + timeout).then([](){
              return cluster::abort_tx_reply();
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
