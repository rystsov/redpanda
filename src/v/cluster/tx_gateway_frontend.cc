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
#include "cluster/id_allocator_frontend.h"
#include <seastar/core/coroutine.hh>

#include "cluster/logger.h"
#include "errc.h"

namespace cluster {

tx_gateway_frontend::tx_gateway_frontend(
    ss::smp_service_group ssg,
    ss::sharded<cluster::partition_manager>& partition_manager,
    ss::sharded<cluster::shard_table>& shard_table,
    ss::sharded<cluster::metadata_cache>& metadata_cache,
    ss::sharded<rpc::connection_cache>& connection_cache,
    ss::sharded<partition_leaders_table>& leaders,
    std::unique_ptr<cluster::controller>& controller,
    ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend)
    : _ssg(ssg)
    , _partition_manager(partition_manager)
    , _shard_table(shard_table)
    , _metadata_cache(metadata_cache)
    , _connection_cache(connection_cache)
    , _leaders(leaders)
    , _controller(controller)
    , _id_allocator_frontend(id_allocator_frontend) {}

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

ss::future<commit_tx_reply>
tx_gateway_frontend::commit_tx(model::ntp ntp, model::producer_identity pid, model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    
    if (!_metadata_cache.local().contains(nt, ntp.tp.partition)) {
        return ss::make_ready_future<commit_tx_reply>(commit_tx_reply {
            .ec = tx_errc::partition_not_exists
        });
    }

    auto leader = _leaders.local().get_leader(ntp);
    if (!leader) {
        vlog(clusterlog.warn, "can't find a leader for {}", ntp);
        return ss::make_ready_future<commit_tx_reply>(commit_tx_reply {
            .ec = tx_errc::leader_not_found
        });
    }

    auto _self = _controller->self();

    if (leader == _self) {
        return do_commit_tx(ntp, pid, timeout);
    }

    vlog(clusterlog.trace, "dispatching commit tx to {} from {}", leader, _self);

    return dispatch_commit_tx(leader.value(), ntp, pid, timeout);
}

ss::future<commit_tx_reply>
tx_gateway_frontend::dispatch_commit_tx(model::node_id leader, model::ntp ntp, model::producer_identity pid, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [ntp, pid, timeout](tx_gateway_client_protocol cp) {
            return cp.commit_tx(
              commit_tx_request{
                  .ntp = ntp,
                  .pid = pid,
                  .timeout = timeout
                },
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<commit_tx_reply>)
      .then([](result<commit_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote abort tx",
                r.error());
              return commit_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<commit_tx_reply>
tx_gateway_frontend::do_commit_tx(model::ntp ntp, model::producer_identity pid, model::timeout_clock::duration timeout) {
    
    auto shard = _shard_table.local().shard_for(ntp);

    if (shard == std::nullopt) {
        vlog(clusterlog.warn, "can't find a shard for {}", ntp);
        return ss::make_ready_future<commit_tx_reply>(commit_tx_reply {
            .ec = tx_errc::shard_not_found
        });
    }

    return _partition_manager.invoke_on(
      *shard, _ssg, [pid, ntp, timeout](cluster::partition_manager& mgr) mutable {
          vlog(clusterlog.warn, "timeout {}", timeout);
          auto partition = mgr.get(ntp);
          if (!partition) {
              vlog(clusterlog.warn, "can't get partition by {} ntp", ntp);
              return ss::make_ready_future<commit_tx_reply>(commit_tx_reply {
                  .ec = tx_errc::partition_not_found
              });
          }

          auto& stm = partition->tx_stm();

          if (!stm) {
              vlog(clusterlog.warn, "can't get tx stm of the {}' partition", ntp);
              return ss::make_ready_future<commit_tx_reply>(commit_tx_reply{
                  .ec = tx_errc::stm_not_found
              });
          }

          // ok, tx not found, timeout
          return stm->commit_tx(pid, model::timeout_clock::now() + timeout).then([](){
              return commit_tx_reply();
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

ss::future<cluster::init_tm_tx_reply>
tx_gateway_frontend::init_tm_tx(kafka::transactional_id tx_id, model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(
      model::kafka_internal_namespace, model::kafka_tx_topic);
    
    if (!_metadata_cache.local().contains(nt, model::partition_id(0))) {
        return ss::make_ready_future<cluster::init_tm_tx_reply>(cluster::init_tm_tx_reply {
            .ec = tx_errc::partition_not_exists
        });
    }

    auto leader = _leaders.local().get_leader(model::kafka_tx_ntp);
    if (!leader) {
        vlog(clusterlog.warn, "can't find a leader for {}", model::kafka_tx_ntp);
        return ss::make_ready_future<init_tm_tx_reply>(cluster::init_tm_tx_reply {
            .ec = tx_errc::leader_not_found
        });
    }

    auto _self = _controller->self();

    if (leader == _self) {
        return do_init_tm_tx(tx_id, timeout);
    }

    vlog(clusterlog.trace, "dispatching abort tx to {} from {}", leader, _self);

    return dispatch_init_tm_tx(leader.value(), tx_id, timeout);
}

ss::future<init_tm_tx_reply>
tx_gateway_frontend::dispatch_init_tm_tx(model::node_id leader, kafka::transactional_id tx_id, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::tx_gateway_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [tx_id, timeout](tx_gateway_client_protocol cp) {
            return cp.init_tm_tx(
              init_tm_tx_request{
                  .tx_id = tx_id,
                  .timeout = timeout
                },
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<init_tm_tx_reply>)
      .then([](result<init_tm_tx_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote abort tx",
                r.error());
              return init_tm_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<cluster::init_tm_tx_reply>
tx_gateway_frontend::do_init_tm_tx(kafka::transactional_id tx_id, model::timeout_clock::duration timeout) {
    auto shard = _shard_table.local().shard_for(model::kafka_tx_ntp);

    if (shard == std::nullopt) {
        vlog(clusterlog.warn, "can't find a shard for {}", model::kafka_tx_ntp);
        return ss::make_ready_future<init_tm_tx_reply>(cluster::init_tm_tx_reply {
            .ec = tx_errc::shard_not_found
        });
    }

    return _partition_manager.invoke_on(
      *shard, _ssg, [this, tx_id, timeout](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(model::kafka_tx_ntp);
          if (!partition) {
              vlog(clusterlog.warn, "can't get partition by {} ntp", model::kafka_tx_ntp);
              return ss::make_ready_future<init_tm_tx_reply>(cluster::init_tm_tx_reply {
                  .ec = tx_errc::partition_not_found
              });
          }

          auto& stm = partition->tm_stm();
          
          if (!stm) {
              vlog(clusterlog.warn, "can't get tm stm of the {}' partition", model::kafka_tx_ntp);
              return ss::make_ready_future<init_tm_tx_reply>(init_tm_tx_reply{
                  .ec = tx_errc::stm_not_found
              });
          }

          auto maybe_tx = stm->get_tx(tx_id);

          if (maybe_tx) {
              auto tx = maybe_tx.value();

              auto f = ss::make_ready_future<checked<tm_transaction, tx_errc>>(tx);

              if (tx.status == tm_transaction::tx_status::ongoing) {
                  f = abort_tm_tx(stm, tx, timeout, ss::make_lw_shared<ss::promise<tx_errc>>());
              } else if (tx.status == tm_transaction::tx_status::preparing) {
                  f = commit_tm_tx(stm, tx, timeout, ss::make_lw_shared<ss::promise<tx_errc>>());
              } else if (tx.status == tm_transaction::tx_status::prepared) {
                  f = recommit_tm_tx(stm, tx, timeout);
              } else if (tx.status == tm_transaction::tx_status::aborting) {
                  f = reabort_tm_tx(stm, tx, timeout);
              } else {
                  vassert(tx.status == tm_transaction::tx_status::finished, "unexpected tx status {}", tx.status);
              }

              return f.then([&stm](checked<tm_transaction, tx_errc> r){
                  if (r.has_value()) {
                      auto tx = r.value();
                      // TODO: check epoch overflow
                      model::producer_identity pid {
                          .id = tx.pid.id,
                          .epoch = static_cast<int16_t>(tx.pid.epoch + 1)
                      };
                      return stm->re_register_producer(tx.id, tx.etag, pid).then([pid](tm_stm::op_status op_status) {
                          init_tm_tx_reply reply {
                              .pid = pid
                          };
                          if (op_status == tm_stm::op_status::success) {
                              reply.ec = tx_errc::success;
                          } else if (op_status == tm_stm::op_status::conflict) {
                              reply.ec = tx_errc::conflict;
                          } else {
                              // timeout or error => result unknown
                              reply.ec = tx_errc::timeout;
                          }
                          return reply;
                      });
                  } else {
                      return ss::make_ready_future<init_tm_tx_reply>(init_tm_tx_reply {
                        .ec = tx_errc::timeout
                      }); 
                  }
              });   
          } else {
              return _id_allocator_frontend.local().allocate_id(timeout).then([&stm, tx_id](allocate_id_reply pid_reply){
                  if (pid_reply.ec == errc::success) {
                      model::producer_identity pid {
                          .id = pid_reply.id,
                          .epoch = 0
                      };
                      return stm->register_new_producer(tx_id, pid).then([pid](tm_stm::op_status op_status) {
                          init_tm_tx_reply reply {
                              .pid = pid
                          };
                          if (op_status == tm_stm::op_status::success) {
                              reply.ec = tx_errc::success;
                          } else if (op_status == tm_stm::op_status::conflict) {
                              reply.ec = tx_errc::conflict;
                          } else {
                              reply.ec = tx_errc::timeout;
                          }
                          return reply;
                      });
                  } else {
                      return ss::make_ready_future<init_tm_tx_reply>(init_tm_tx_reply {
                        .ec = tx_errc::timeout
                      });
                  }
              });
          }
      });
}

ss::future<checked<cluster::tm_transaction, tx_errc>>
tx_gateway_frontend::abort_tm_tx(ss::shared_ptr<cluster::tm_stm>& stm, cluster::tm_transaction tx, model::timeout_clock::duration timeout, ss::lw_shared_ptr<ss::promise<tx_errc>> outcome) {
    auto changed_tx = co_await stm->try_change_status(tx.id, tx.etag, cluster::tm_transaction::tx_status::aborting);
    if (!changed_tx.has_value()) {
        // todo support other error types
        outcome->set_value(tx_errc::timeout);
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    outcome->set_value(tx_errc::success);

    tx = changed_tx.value();
    std::vector<ss::future<abort_tx_reply>> fs;
    for (auto rm : tx.partitions) {
        fs.push_back(abort_tx(rm.ntp, tx.pid, timeout));
    }
    auto rs = co_await when_all_succeed(fs.begin(), fs.end());
    bool ok = true;
    for (auto r : rs) {
        // TODO: support partition removal, maybe treat it as success?
        ok = ok && (r.ec == tx_errc::success);
    }
    if (!ok) {
        // todo: use sane errors
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    co_return checked<cluster::tm_transaction, tx_errc>(tx);
}

ss::future<checked<cluster::tm_transaction, tx_errc>>
tx_gateway_frontend::commit_tm_tx(ss::shared_ptr<cluster::tm_stm>& stm, cluster::tm_transaction tx, model::timeout_clock::duration timeout, ss::lw_shared_ptr<ss::promise<tx_errc>> outcome) {
    std::vector<ss::future<prepare_tx_reply>> pfs;
    for (auto rm : tx.partitions) {
        pfs.push_back(prepare_tx(rm.ntp, rm.etag, tx.pid, timeout));
    }

    if (tx.status == tm_transaction::tx_status::ongoing) {
        auto became_preparing_tx = co_await stm->try_change_status(tx.id, tx.etag, cluster::tm_transaction::tx_status::preparing);
        if (!became_preparing_tx.has_value()) {
            // todo: use sane errors
            outcome->set_value(tx_errc::timeout);
            co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
        }
        tx = became_preparing_tx.value();
    }
    
    auto prs = co_await when_all_succeed(pfs.begin(), pfs.end());
    bool ok = true;
    for (auto r : prs) {
        // TODO: support partition removal, maybe treat it as success?
        // TODO: handle etag violations => abort
        ok = ok && (r.ec == tx_errc::success);
    }
    if (!ok) {
        // todo: use sane errors
        outcome->set_value(tx_errc::timeout);
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    outcome->set_value(tx_errc::success);
    
    auto changed_tx = co_await stm->try_change_status(tx.id, tx.etag, cluster::tm_transaction::tx_status::prepared);
    if (!changed_tx.has_value()) {
        // todo support other error types
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    tx = changed_tx.value();

    std::vector<ss::future<commit_tx_reply>> cfs;
    for (auto rm : tx.partitions) {
        cfs.push_back(commit_tx(rm.ntp, tx.pid, timeout));
    }
    auto crs = co_await when_all_succeed(cfs.begin(), cfs.end());
    ok = true;
    for (auto r : crs) {
        ok = ok && (r.ec == tx_errc::success);
    }
    if (ok) {
        co_return checked<cluster::tm_transaction, tx_errc>(tx);
    }
    co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
}

ss::future<checked<tm_transaction, tx_errc>>
tx_gateway_frontend::recommit_tm_tx(ss::shared_ptr<tm_stm>& stm, tm_transaction tx, [[maybe_unused]] model::timeout_clock::duration timeout) {
    std::vector<ss::future<commit_tx_reply>> cfs;
    for (auto rm : tx.partitions) {
        cfs.push_back(commit_tx(rm.ntp, tx.pid, timeout));
    }
    auto crs = co_await when_all_succeed(cfs.begin(), cfs.end());
    auto ok = true;
    for (auto r : crs) {
        ok = ok && (r.ec == tx_errc::success);
    }
    if (ok) {
        co_return checked<tm_transaction, tx_errc>(tx);
    }
    co_return checked<tm_transaction, tx_errc>(tx_errc::timeout);
}

ss::future<checked<tm_transaction, tx_errc>>
tx_gateway_frontend::reabort_tm_tx(ss::shared_ptr<tm_stm>& stm, tm_transaction tx, [[maybe_unused]] model::timeout_clock::duration timeout) {
    std::vector<ss::future<abort_tx_reply>> cfs;
    for (auto rm : tx.partitions) {
        cfs.push_back(abort_tx(rm.ntp, tx.pid, timeout));
    }
    auto crs = co_await when_all_succeed(cfs.begin(), cfs.end());
    auto ok = true;
    for (auto r : crs) {
        ok = ok && (r.ec == tx_errc::success);
    }
    if (ok) {
        co_return checked<tm_transaction, tx_errc>(tx);
    }
    co_return checked<tm_transaction, tx_errc>(tx_errc::timeout);
}

ss::future<kafka::add_partitions_to_txn_response_data>
tx_gateway_frontend::add_partition_to_tx(kafka::add_partitions_to_txn_request_data request, [[maybe_unused]] model::timeout_clock::duration timeout) {
    kafka::add_partitions_to_txn_response_data response;
    for (auto& req_topic : request.topics) {
        kafka::add_partitions_to_txn_topic_result res_topic;
        res_topic.name = req_topic.name;
        for (int32_t req_partition : req_topic.partitions) {
            kafka::add_partitions_to_txn_partition_result res_partition;
            res_partition.partition_index = req_partition;
            res_partition.error_code = kafka::error_code::none;
            res_topic.results.push_back(res_partition);
        }
        response.results.push_back(res_topic);
    }
    return ss::make_ready_future<kafka::add_partitions_to_txn_response_data>(response);
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
