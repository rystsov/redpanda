// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_group_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/id_allocator_frontend.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group.h"
#include "kafka/server/group_router.h"
#include <seastar/core/coroutine.hh>
#include <algorithm>

#include "cluster/logger.h"
#include "errc.h"

namespace cluster {
using namespace std::chrono_literals;

rm_group_frontend::rm_group_frontend(
    ss::sharded<cluster::metadata_cache>& metadata_cache,
    ss::sharded<rpc::connection_cache>& connection_cache,
    ss::sharded<partition_leaders_table>& leaders,
    std::unique_ptr<cluster::controller>& controller,
    ss::sharded<kafka::coordinator_ntp_mapper>& coordinator_mapper,
    ss::sharded<kafka::group_router>& group_router)
    : _metadata_cache(metadata_cache)
    , _connection_cache(connection_cache)
    , _leaders(leaders)
    , _controller(controller)
    , _coordinator_mapper(coordinator_mapper)
    , _group_router(group_router)
    , _metadata_dissemination_retries(config::shard_local_cfg().metadata_dissemination_retries.value())
    , _metadata_dissemination_retry_delay_ms(config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value()) {}

ss::future<begin_group_tx_reply>
rm_group_frontend::begin_group_tx(kafka::group_id group_id, model::producer_identity pid, model::timeout_clock::duration timeout) {
    auto ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
    if (!ntp_opt) {
        vlog(clusterlog.warn, "can't find ntp for {}, creating a consumer group topic", group_id);
        auto has_created = co_await try_create_consumer_group_topic();
        if (!has_created) {
            vlog(clusterlog.warn, "can't create consumer group topic", group_id);
            co_return begin_group_tx_reply {
                .ec = tx_errc::partition_not_exists
            };
        }
    }

    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    std::optional<model::node_id> leader_opt = std::nullopt;
    tx_errc ec;
    while (!leader_opt && 0 < retries--) {
        ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
        if (!ntp_opt) {
            vlog(clusterlog.warn, "can't find ntp for {}, retrying", group_id);
            ec = tx_errc::partition_not_exists;
            co_await ss::sleep(delay_ms);
            continue;
        }

        auto ntp = ntp_opt.value();
        auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
        if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
            vlog(clusterlog.info, "can't find meta info for {}, retrying", ntp);
            ec = tx_errc::partition_not_exists;
            co_await ss::sleep(delay_ms);
            continue;
        }

        leader_opt = _leaders.local().get_leader(ntp);
        if (!leader_opt) {
            vlog(clusterlog.warn, "can't find a leader for {}", ntp);
            ec = tx_errc::leader_not_found;
            co_await ss::sleep(delay_ms);
        }
    }

    if (!leader_opt) {
        co_return begin_group_tx_reply {
            .ec = ec
        };
    }
    
    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        co_return co_await do_begin_group_tx(group_id, pid, timeout);
    }

    vlog(clusterlog.trace, "dispatching begin group tx to {} from {}", leader, _self);
    co_return co_await dispatch_begin_group_tx(leader, group_id, pid, timeout);
}

ss::future<begin_group_tx_reply>
rm_group_frontend::dispatch_begin_group_tx(model::node_id leader, kafka::group_id group_id, model::producer_identity pid, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [group_id, pid, timeout](tx_gateway_client_protocol cp) {
            return cp.begin_group_tx(
              begin_group_tx_request{
                  .group_id = group_id,
                  .pid = pid,
                  .timeout = timeout
                },
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<begin_group_tx_reply>)
      .then([](result<begin_group_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote begin group tx",
                r.error());
              return begin_group_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<begin_group_tx_reply>
rm_group_frontend::do_begin_group_tx(kafka::group_id group_id, model::producer_identity pid, model::timeout_clock::duration timeout) {
    begin_group_tx_request req;
    req.group_id = group_id;
    req.pid = pid;
    req.timeout = timeout;
    co_return co_await _group_router.local().begin_tx(std::move(req));
}

ss::future<prepare_group_tx_reply>
rm_group_frontend::prepare_group_tx(kafka::group_id group_id, model::term_id etag, model::producer_identity pid, model::tx_seq tx_seq, model::timeout_clock::duration timeout) {
    auto ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
    if (!ntp_opt) {
        vlog(clusterlog.warn, "can't find ntp for {} ", group_id);
        co_return prepare_group_tx_reply {
            .ec = tx_errc::partition_not_exists
        };
    }
    auto ntp = ntp_opt.value();

    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        vlog(clusterlog.info, "can't find meta info for {}", ntp);
        co_return prepare_group_tx_reply {
            .ec = tx_errc::partition_not_exists
        };
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        vlog(clusterlog.warn, "can't find a leader for {}", ntp);
        co_return prepare_group_tx_reply {
            .ec = tx_errc::leader_not_found
        };
    }
    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        co_return co_await do_prepare_group_tx(group_id, etag, pid, tx_seq, timeout);
    }

    vlog(clusterlog.trace, "dispatching begin group tx to {} from {}", leader, _self);
    co_return co_await dispatch_prepare_group_tx(leader, group_id, etag, pid, tx_seq, timeout);
}

ss::future<prepare_group_tx_reply>
rm_group_frontend::dispatch_prepare_group_tx(model::node_id leader, kafka::group_id group_id, model::term_id etag, model::producer_identity pid, model::tx_seq tx_seq, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [group_id, etag, pid, tx_seq, timeout](tx_gateway_client_protocol cp) {
            return cp.prepare_group_tx(
              prepare_group_tx_request{
                  .group_id = group_id,
                  .etag = etag,
                  .pid = pid,
                  .tx_seq = tx_seq,
                  .timeout = timeout
                },
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<prepare_group_tx_reply>)
      .then([](result<prepare_group_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote begin group tx",
                r.error());
              return prepare_group_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<prepare_group_tx_reply>
rm_group_frontend::do_prepare_group_tx(kafka::group_id group_id, model::term_id etag, model::producer_identity pid, model::tx_seq tx_seq, model::timeout_clock::duration timeout) {
    prepare_group_tx_request req;
    req.group_id = group_id;
    req.etag = etag;
    req.pid = pid;
    req.tx_seq = tx_seq;
    req.timeout = timeout;

    co_return co_await _group_router.local().prepare_tx(std::move(req));
}

ss::future<commit_group_tx_reply>
rm_group_frontend::commit_group_tx(kafka::group_id group_id, model::producer_identity pid, model::tx_seq tx_seq, model::timeout_clock::duration timeout) {
    auto ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
    if (!ntp_opt) {
        vlog(clusterlog.info, "can't find ntp for {}", group_id);
        co_return commit_group_tx_reply {
            .ec = tx_errc::partition_not_exists
        };
    }

    auto ntp = ntp_opt.value();

    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        vlog(clusterlog.info, "can' find meta info for {}", ntp);
        co_return commit_group_tx_reply {
            .ec = tx_errc::partition_not_exists
        };
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        vlog(clusterlog.warn, "can't find a leader for {}", ntp);
        co_return commit_group_tx_reply {
            .ec = tx_errc::leader_not_found
        };
    }
    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        co_return co_await do_commit_group_tx(group_id, pid, tx_seq, timeout);
    }

    vlog(clusterlog.trace, "dispatching commit group tx to {} from {}", leader, _self);
    co_return co_await dispatch_commit_group_tx(leader, group_id, pid, tx_seq, timeout);
}

ss::future<commit_group_tx_reply>
rm_group_frontend::dispatch_commit_group_tx(model::node_id leader, kafka::group_id group_id, model::producer_identity pid, model::tx_seq tx_seq, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [group_id, pid, tx_seq, timeout](tx_gateway_client_protocol cp) {
            return cp.commit_group_tx(
              commit_group_tx_request{
                  .pid = pid,
                  .tx_seq = tx_seq,
                  .group_id = group_id,
                  .timeout = timeout
                },
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<commit_group_tx_reply>)
      .then([](result<commit_group_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote abort tx",
                r.error());
              return commit_group_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<commit_group_tx_reply>
rm_group_frontend::do_commit_group_tx(kafka::group_id group_id, model::producer_identity pid, model::tx_seq tx_seq, model::timeout_clock::duration timeout) {
    commit_group_tx_request req;
    req.group_id = group_id;
    req.pid = pid;
    req.tx_seq = tx_seq;
    req.timeout = timeout;

    co_return co_await _group_router.local().commit_tx(std::move(req));
}

ss::future<abort_group_tx_reply>
rm_group_frontend::abort_group_tx(kafka::group_id group_id, model::producer_identity pid, model::timeout_clock::duration timeout) {
    auto ntp_opt = _coordinator_mapper.local().ntp_for(group_id);
    if (!ntp_opt) {
        vlog(clusterlog.warn, "can't find ntp for {} ", group_id);
        co_return abort_group_tx_reply {
            .ec = tx_errc::partition_not_exists
        };
    }
    auto ntp = ntp_opt.value();

    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        vlog(clusterlog.info, "can't find meta info for {}", ntp);
        co_return abort_group_tx_reply {
            .ec = tx_errc::partition_not_exists
        };
    }

    auto leader_opt = _leaders.local().get_leader(ntp);
    if (!leader_opt) {
        vlog(clusterlog.warn, "can't find a leader for {}", ntp);
        co_return abort_group_tx_reply {
            .ec = tx_errc::leader_not_found
        };
    }
    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        co_return co_await do_abort_group_tx(group_id, pid, timeout);
    }

    vlog(clusterlog.trace, "dispatching begin group tx to {} from {}", leader, _self);
    co_return co_await dispatch_abort_group_tx(leader, group_id, pid, timeout);
}

ss::future<abort_group_tx_reply>
rm_group_frontend::dispatch_abort_group_tx(model::node_id leader, kafka::group_id group_id, model::producer_identity pid, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [group_id, pid, timeout](tx_gateway_client_protocol cp) {
            return cp.abort_group_tx(
              abort_group_tx_request{
                  .group_id = group_id,
                  .pid = pid,
                  .timeout = timeout
                },
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<abort_group_tx_reply>)
      .then([](result<abort_group_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote begin group tx",
                r.error());
              return abort_group_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<abort_group_tx_reply>
rm_group_frontend::do_abort_group_tx(kafka::group_id group_id, model::producer_identity pid, model::timeout_clock::duration timeout) {
    abort_group_tx_request req;
    req.group_id = group_id;
    req.pid = pid;
    req.timeout = timeout;

    co_return co_await _group_router.local().abort_tx(std::move(req));
}

ss::future<bool>
rm_group_frontend::try_create_consumer_group_topic() {
    cluster::topic_configuration topic{
    _coordinator_mapper.local().ns(),
    _coordinator_mapper.local().topic(),
    config::shard_local_cfg().group_topic_partitions(),
    config::shard_local_cfg().default_topic_replication()};

    topic.properties.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

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
          vlog(clusterlog.warn, "cant create consumer group topic {}", e);
          return false;
      });
}

} // namespace cluster
