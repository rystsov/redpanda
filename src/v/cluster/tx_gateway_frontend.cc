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
#include "cluster/id_allocator_frontend.h"
#include "cluster/rm_group_frontend.h"
#include "cluster/rm_partition_frontend.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group.h"
#include "kafka/server/group_router.h"
#include <seastar/core/coroutine.hh>
#include <algorithm>

#include "cluster/logger.h"
#include "errc.h"

namespace cluster {
using namespace std::chrono_literals;

static kafka::add_partitions_to_txn_response_data
make_add_partitions_error_response(kafka::add_partitions_to_txn_request_data request, kafka::error_code ec) {
    kafka::add_partitions_to_txn_response_data response;
    for (auto& req_topic : request.topics) {
        kafka::add_partitions_to_txn_topic_result res_topic;
        res_topic.name = req_topic.name;
        for (int32_t req_partition : req_topic.partitions) {
            kafka::add_partitions_to_txn_partition_result res_partition;
            res_partition.partition_index = req_partition;
            res_partition.error_code = ec;
            res_topic.results.push_back(res_partition);
        }
        response.results.push_back(res_topic);
    }
    return response;
}

tx_gateway_frontend::tx_gateway_frontend(
    ss::smp_service_group ssg,
    ss::sharded<cluster::partition_manager>& partition_manager,
    ss::sharded<cluster::shard_table>& shard_table,
    ss::sharded<cluster::metadata_cache>& metadata_cache,
    ss::sharded<rpc::connection_cache>& connection_cache,
    ss::sharded<partition_leaders_table>& leaders,
    std::unique_ptr<cluster::controller>& controller,
    ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend,
    ss::sharded<cluster::rm_group_frontend>& rm_group_frontend,
    ss::sharded<cluster::rm_partition_frontend>& rm_partition_frontend,
    ss::sharded<kafka::coordinator_ntp_mapper>& coordinator_mapper,
    ss::sharded<kafka::group_router>& group_router)
    : _ssg(ssg)
    , _partition_manager(partition_manager)
    , _shard_table(shard_table)
    , _metadata_cache(metadata_cache)
    , _connection_cache(connection_cache)
    , _leaders(leaders)
    , _controller(controller)
    , _id_allocator_frontend(id_allocator_frontend)
    , _rm_group_frontend(rm_group_frontend)
    , _rm_partition_frontend(rm_partition_frontend)
    , _coordinator_mapper(coordinator_mapper)
    , _group_router(group_router)
    , _metadata_dissemination_retries(config::shard_local_cfg().metadata_dissemination_retries.value())
    , _metadata_dissemination_retry_delay_ms(config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value()) {}

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

ss::future<cluster::init_tm_tx_reply>
tx_gateway_frontend::init_tm_tx(kafka::transactional_id tx_id, model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(
      model::kafka_internal_namespace, model::kafka_tx_topic);
    
    if (!_metadata_cache.local().contains(nt, model::partition_id(0))) {
        vlog(clusterlog.warn, "can't find {}/0 partition", nt);
        co_return cluster::init_tm_tx_reply {
            .ec = tx_errc::partition_not_exists
        };
    }

    auto leader_opt = _leaders.local().get_leader(model::kafka_tx_ntp);

    auto i=0;
    while (!leader_opt && i<30) {
        co_await ss::sleep(0'500ms);
        leader_opt = _leaders.local().get_leader(model::kafka_tx_ntp);
    }

    if (!leader_opt) {
        vlog(clusterlog.warn, "can't find a leader for {}", model::kafka_tx_ntp);
        co_return cluster::init_tm_tx_reply {
            .ec = tx_errc::leader_not_found
        };
    }
    
    auto leader = leader_opt.value();
    auto _self = _controller->self();

    if (leader == _self) {
        co_return co_await do_init_tm_tx(tx_id, timeout);
    }

    vlog(clusterlog.trace, "dispatching abort tx to {} from {}", leader, _self);

    co_return co_await dispatch_init_tm_tx(leader, tx_id, timeout);
}

ss::future<cluster::init_tm_tx_reply>
tx_gateway_frontend::init_tm_tx_locally(kafka::transactional_id tx_id, model::timeout_clock::duration timeout) {
    auto shard = _shard_table.local().shard_for(model::kafka_tx_ntp);

    auto i=0;
    while (!shard && i<10) {
        co_await ss::sleep(0'100ms);
        shard = _shard_table.local().shard_for(model::kafka_tx_ntp);
    }

    if (!shard) {
        vlog(clusterlog.warn, "can't find a shard for {}", model::kafka_tx_ntp);
        co_return cluster::init_tm_tx_reply {
            .ec = tx_errc::shard_not_found
        };
    }

    co_return co_await do_init_tm_tx(*shard, tx_id, timeout);
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
                "got error {} on remote init tm tx",
                r.error());
              return init_tm_tx_reply{ .ec = tx_errc::timeout};
          }

          return r.value();
      });
}

ss::future<cluster::init_tm_tx_reply>
tx_gateway_frontend::do_init_tm_tx(ss::shard_id shard, kafka::transactional_id tx_id, model::timeout_clock::duration timeout) {
    return container().invoke_on(shard, _ssg, [tx_id, timeout](tx_gateway_frontend& self) {
        return self.do_init_tm_tx(tx_id, timeout);
    });
}

ss::future<cluster::init_tm_tx_reply>
tx_gateway_frontend::do_init_tm_tx(kafka::transactional_id tx_id, model::timeout_clock::duration timeout) {
    auto partition = _partition_manager.local().get(model::kafka_tx_ntp);
    if (!partition) {
        vlog(clusterlog.warn, "can't get partition by {} ntp", model::kafka_tx_ntp);
        co_return cluster::init_tm_tx_reply {
            .ec = tx_errc::partition_not_found
        };
    }

    auto& stm = partition->tm_stm();

    if (!stm) {
        vlog(clusterlog.warn, "can't get tm stm of the {}' partition", model::kafka_tx_ntp);
        co_return init_tm_tx_reply {
            .ec = tx_errc::stm_not_found
        };
    }

    auto maybe_tx = stm->get_tx(tx_id);

    if (!maybe_tx) {
        allocate_id_reply pid_reply = co_await _id_allocator_frontend.local().allocate_id(timeout);
        if (pid_reply.ec != errc::success) {
            vlog(clusterlog.warn, "allocate_id failed with {}", pid_reply.ec);
            co_return init_tm_tx_reply { .ec = tx_errc::timeout };
        }
            
        model::producer_identity pid {
            .id = pid_reply.id,
            .epoch = 0
        };
        tm_stm::op_status op_status = co_await stm->register_new_producer(tx_id, pid);
        init_tm_tx_reply reply { .pid = pid };
        if (op_status == tm_stm::op_status::success) {
            reply.ec = tx_errc::none;
        } else if (op_status == tm_stm::op_status::conflict) {
            vlog(clusterlog.warn, "can't register new producer status: {}", op_status);
            reply.ec = tx_errc::conflict;
        } else {
            vlog(clusterlog.warn, "can't register new producer status: {}", op_status);
            reply.ec = tx_errc::timeout;
        }
        co_return reply;
    }

    auto tx = maybe_tx.value();

    checked<tm_transaction, tx_errc> r(tx);

    if (tx.status == tm_transaction::tx_status::ongoing) {
        r = co_await abort_tm_tx(stm, tx, timeout, ss::make_lw_shared<ss::promise<tx_errc>>());
    } else if (tx.status == tm_transaction::tx_status::preparing) {
        r = co_await commit_tm_tx(stm, tx, timeout, ss::make_lw_shared<ss::promise<tx_errc>>());
    } else if (tx.status == tm_transaction::tx_status::prepared) {
        r = co_await recommit_tm_tx(stm, tx, timeout);
    } else if (tx.status == tm_transaction::tx_status::aborting) {
        r = co_await reabort_tm_tx(stm, tx, timeout);
    } else {
        vassert(tx.status == tm_transaction::tx_status::finished, "unexpected tx status {}", tx.status);
    }

    if (!r.has_value()) {
        co_return init_tm_tx_reply { .ec = tx_errc::timeout };
    }

    tx = r.value();
    init_tm_tx_reply reply;
    if (tx.pid.epoch < std::numeric_limits<int16_t>::max()) {
        reply.pid = model::producer_identity {
            .id = tx.pid.id,
            .epoch = static_cast<int16_t>(tx.pid.epoch + 1)
        };
    } else {
        allocate_id_reply pid_reply = co_await _id_allocator_frontend.local().allocate_id(timeout);
        reply.pid = model::producer_identity {
            .id = pid_reply.id,
            .epoch = 0
        };
    }

    tm_stm::op_status op_status = co_await stm->re_register_producer(tx.id, tx.etag, reply.pid);
    if (op_status == tm_stm::op_status::success) {
        reply.ec = tx_errc::none;
    } else if (op_status == tm_stm::op_status::conflict) {
        reply.ec = tx_errc::conflict;
    } else {
        reply.ec = tx_errc::timeout;
    }
    co_return reply;
}

ss::future<kafka::add_partitions_to_txn_response_data>
tx_gateway_frontend::add_partition_to_tx(kafka::add_partitions_to_txn_request_data request, model::timeout_clock::duration timeout) {
    auto shard = _shard_table.local().shard_for(model::kafka_tx_ntp);

    if (shard == std::nullopt) {
        vlog(clusterlog.warn, "can't find a shard for {}", model::kafka_tx_ntp);
        return ss::make_ready_future<kafka::add_partitions_to_txn_response_data>(
            make_add_partitions_error_response(request, kafka::error_code::unknown_server_error));
    }

    return container().invoke_on(*shard, _ssg, [request, timeout](tx_gateway_frontend& self) {
        auto partition = self._partition_manager.local().get(model::kafka_tx_ntp);
        if (!partition) {
            vlog(clusterlog.warn, "can't get partition by {} ntp", model::kafka_tx_ntp);
            return ss::make_ready_future<kafka::add_partitions_to_txn_response_data>(
                make_add_partitions_error_response(request, kafka::error_code::unknown_server_error));
        }

        auto stm = partition->tm_stm();

        if (!stm) {
            vlog(clusterlog.warn, "can't get tm stm of the {}' partition", model::kafka_tx_ntp);
            return ss::make_ready_future<kafka::add_partitions_to_txn_response_data>(
                make_add_partitions_error_response(request, kafka::error_code::unknown_server_error));
        }

        auto maybe_tx = stm->get_tx(request.transactional_id);
        if (!maybe_tx) {
            return ss::make_ready_future<kafka::add_partitions_to_txn_response_data>(
                make_add_partitions_error_response(request, kafka::error_code::unknown_server_error));
        }

        return ss::do_with(stm, [&self, request, timeout](auto& stm) {
            return stm->get_end_lock(request.transactional_id)->with([&self, &stm, request, timeout](){
                return self.do_add_partition_to_tx(stm, request, timeout);
            });
        });
    });
}

ss::future<kafka::add_partitions_to_txn_response_data>
tx_gateway_frontend::do_add_partition_to_tx(ss::shared_ptr<tm_stm>& stm, kafka::add_partitions_to_txn_request_data request, model::timeout_clock::duration timeout) {
    model::producer_identity pid {
        .id = request.producer_id,
        .epoch = request.producer_epoch
    };

    auto f = get_tx(stm, pid, request.transactional_id, timeout);

    return f.then([this, pid, request, timeout, &stm](checked<tm_transaction, tx_errc> r){
        if (!r.has_value()) {
            return ss::make_ready_future<kafka::add_partitions_to_txn_response_data>(
              make_add_partitions_error_response(request, kafka::error_code::unknown_server_error));
        }

        kafka::add_partitions_to_txn_response_data response;
        if (request.topics.size()==0) {
            return ss::make_ready_future<kafka::add_partitions_to_txn_response_data>(response);
        }
    
        auto tx = r.value();
        
        std::vector<ss::future<begin_tx_reply>> bfs;
        
        for (auto& req_topic : request.topics) {
            kafka::add_partitions_to_txn_topic_result res_topic;
            res_topic.name = req_topic.name;
      
            model::topic topic(req_topic.name);
        
            for (int32_t req_partition : req_topic.partitions) {
                model::ntp ntp(model::kafka_namespace,topic, model::partition_id(req_partition));
                auto has_ntp = std::any_of(tx.partitions.begin(), tx.partitions.end(), [ntp](const auto& rm) {
                    return rm.ntp == ntp;
                });
                if (has_ntp) {
                    kafka::add_partitions_to_txn_partition_result res_partition;
                    res_partition.partition_index = req_partition;
                    res_partition.error_code = kafka::error_code::none;
                    res_topic.results.push_back(res_partition);
                } else {
                    bfs.push_back(_rm_partition_frontend.local().begin_tx(ntp, pid, timeout));
                }
            }
            response.results.push_back(res_topic);
        }
        
        return when_all_succeed(bfs.begin(), bfs.end()).then([response, tx, &stm](std::vector<begin_tx_reply> brs) mutable {
            std::vector<tm_transaction::tx_partition> partitions;
            for (auto& br : brs) {
                auto topic_it = std::find_if(response.results.begin(), response.results.end(), [&br](const auto& r) {
                    return r.name == br.ntp.tp.topic();
                });
                vassert(topic_it != response.results.end(), "can't find expected topic {}", br.ntp.tp.topic());
                vassert(std::none_of(topic_it->results.begin(), topic_it->results.end(), [&br](const auto& r) { 
                    return r.partition_index == br.ntp.tp.partition();
                }), "partition {} is already part of the response", br.ntp.tp.partition());
                if (br.ec == tx_errc::none) {
                    partitions.push_back(tm_transaction::tx_partition {
                        .ntp = br.ntp,
                        .etag = br.etag
                    });
                }
            }
            auto has_added = stm->add_partitions(tx.id, tx.etag, partitions);
            for (auto& br : brs) {
                auto topic_it = std::find_if(response.results.begin(), response.results.end(), [&br](const auto& r) {
                    return r.name == br.ntp.tp.topic();
                });
                
                kafka::add_partitions_to_txn_partition_result res_partition;
                res_partition.partition_index = br.ntp.tp.partition();
                if (has_added && br.ec == tx_errc::none) {
                    res_partition.error_code = kafka::error_code::none;
                } else {
                    res_partition.error_code = kafka::error_code::unknown_server_error;
                }
                topic_it->results.push_back(res_partition);
            }
            return response;
        });
    });
}

ss::future<kafka::add_offsets_to_txn_response_data>
tx_gateway_frontend::add_offsets_to_tx(kafka::add_offsets_to_txn_request_data request, model::timeout_clock::duration timeout) {
    auto shard = _shard_table.local().shard_for(model::kafka_tx_ntp);

    if (shard == std::nullopt) {
        vlog(clusterlog.warn, "can't find a shard for {}", model::kafka_tx_ntp);
        return ss::make_ready_future<kafka::add_offsets_to_txn_response_data>(kafka::add_offsets_to_txn_response_data {
            .error_code = kafka::error_code::unknown_server_error
        });
    }

    return container().invoke_on(*shard, _ssg, [request, timeout](tx_gateway_frontend& self) {
        auto partition = self._partition_manager.local().get(model::kafka_tx_ntp);
        if (!partition) {
            vlog(clusterlog.warn, "can't get partition by {} ntp", model::kafka_tx_ntp);
            return ss::make_ready_future<kafka::add_offsets_to_txn_response_data>(kafka::add_offsets_to_txn_response_data {
                .error_code = kafka::error_code::unknown_server_error
            });
        }

        auto stm = partition->tm_stm();

        if (!stm) {
            vlog(clusterlog.warn, "can't get tm stm of the {}' partition", model::kafka_tx_ntp);
            return ss::make_ready_future<kafka::add_offsets_to_txn_response_data>(kafka::add_offsets_to_txn_response_data {
                .error_code = kafka::error_code::unknown_server_error
            });
        }

        auto maybe_tx = stm->get_tx(request.transactional_id);
        if (!maybe_tx) {
            return ss::make_ready_future<kafka::add_offsets_to_txn_response_data>(kafka::add_offsets_to_txn_response_data {
                .error_code = kafka::error_code::unknown_server_error
            });
        }

        return ss::do_with(stm, [&self, request, timeout](auto& stm) {
            return stm->get_end_lock(request.transactional_id)->with([&self, &stm, request, timeout](){
                return self.do_add_offsets_to_tx(stm, request, timeout);
            });
        });
    });
}

ss::future<kafka::add_offsets_to_txn_response_data>
tx_gateway_frontend::do_add_offsets_to_tx(ss::shared_ptr<tm_stm>& stm, kafka::add_offsets_to_txn_request_data request, model::timeout_clock::duration timeout) {
    model::producer_identity pid {
        .id = request.producer_id,
        .epoch = request.producer_epoch
    };

    auto tx_opt = co_await get_tx(stm, pid, request.transactional_id, timeout);
    if (!tx_opt.has_value()) {
        co_return kafka::add_offsets_to_txn_response_data {
            .error_code = kafka::error_code::unknown_server_error
        };
    }

    auto group_info = co_await _rm_group_frontend.local().begin_group_tx(request.group_id, pid, timeout);
    if (group_info.ec != tx_errc::none) {
        auto ec = kafka::error_code::unknown_server_error;
        if (group_info.ec == tx_errc::not_coordinator) {
            ec = kafka::error_code::not_coordinator;
        } else if (group_info.ec == tx_errc::coordinator_load_in_progress) {
            ec = kafka::error_code::coordinator_load_in_progress;
        }
        vlog(clusterlog.warn, "error on begining group tx: {}", group_info.ec);
        co_return kafka::add_offsets_to_txn_response_data {
            .error_code = ec
        };
    }
    
    auto tx = tx_opt.value();
    auto has_added = stm->add_group(tx.id, tx.etag, request.group_id, group_info.etag);
    if (!has_added) {
        vlog(clusterlog.warn, "can't add group to tm_stm");
        co_return kafka::add_offsets_to_txn_response_data {
            .error_code = kafka::error_code::unknown_server_error
        };
    }
    co_return kafka::add_offsets_to_txn_response_data {
        .error_code = kafka::error_code::none
    };
}

ss::future<kafka::end_txn_response_data>
tx_gateway_frontend::end_txn(kafka::end_txn_request_data request, model::timeout_clock::duration timeout) {
    auto shard = _shard_table.local().shard_for(model::kafka_tx_ntp);

    if (shard == std::nullopt) {
        vlog(clusterlog.warn, "can't find a shard for {}", model::kafka_tx_ntp);
        return ss::make_ready_future<kafka::end_txn_response_data>(kafka::end_txn_response_data {
            .error_code = kafka::error_code::unknown_server_error
        });
    }

    return container().invoke_on(*shard, _ssg, [request, timeout](tx_gateway_frontend& self) {
        auto partition = self._partition_manager.local().get(model::kafka_tx_ntp);
        if (!partition) {
            vlog(clusterlog.warn, "can't get partition by {} ntp", model::kafka_tx_ntp);
            return ss::make_ready_future<kafka::end_txn_response_data>(kafka::end_txn_response_data {
                .error_code = kafka::error_code::unknown_server_error
            });
        }

        auto& stm = partition->tm_stm();

        if (!stm) {
            vlog(clusterlog.warn, "can't get tm stm of the {}' partition", model::kafka_tx_ntp);
            return ss::make_ready_future<kafka::end_txn_response_data>(kafka::end_txn_response_data {
                .error_code = kafka::error_code::unknown_server_error
            });
        }

        auto maybe_tx = stm->get_tx(request.transactional_id);
        if (!maybe_tx) {
            return ss::make_ready_future<kafka::end_txn_response_data>(kafka::end_txn_response_data {
                .error_code = kafka::error_code::unknown_server_error
            });
        }

        auto tx = maybe_tx.value();
        if (tx.status != tm_transaction::tx_status::ongoing) {
            return ss::make_ready_future<kafka::end_txn_response_data>(kafka::end_txn_response_data {
                .error_code = kafka::error_code::unknown_server_error
            });
        }

        model::producer_identity pid {
            .id = request.producer_id,
            .epoch = request.producer_epoch
        };
        if (tx.pid != pid) {
            if (tx.pid.id == pid.id && tx.pid.epoch > pid.epoch) {
                return ss::make_ready_future<kafka::end_txn_response_data>(kafka::end_txn_response_data {
                    .error_code = kafka::error_code::invalid_producer_epoch
                });
            }

            return ss::make_ready_future<kafka::end_txn_response_data>(kafka::end_txn_response_data {
                .error_code = kafka::error_code::unknown_server_error
            });
        }

        auto outcome = ss::make_lw_shared<ss::promise<tx_errc>>();
        if (request.committed) {    
            (void)self.commit_tm_tx(stm, tx, timeout, outcome);
        } else {
            (void)self.abort_tm_tx(stm, tx, timeout, outcome);
        }

        return outcome->get_future().then([](tx_errc ec) {
            if (ec == tx_errc::none) {
                return kafka::end_txn_response_data {
                .error_code = kafka::error_code::none
                };
            }

            return kafka::end_txn_response_data {
            .error_code = kafka::error_code::unknown_server_error
            };
        });
    });
}

ss::future<checked<cluster::tm_transaction, tx_errc>>
tx_gateway_frontend::abort_tm_tx(ss::shared_ptr<cluster::tm_stm>& stm, cluster::tm_transaction tx, model::timeout_clock::duration timeout, ss::lw_shared_ptr<ss::promise<tx_errc>> outcome) {
    return stm->get_end_lock(tx.id)->with([this, &stm, timeout, tx, outcome](){
        return do_abort_tm_tx(stm, tx, timeout, outcome);
    });
}

ss::future<checked<cluster::tm_transaction, tx_errc>>
tx_gateway_frontend::do_abort_tm_tx(ss::shared_ptr<cluster::tm_stm>& stm, cluster::tm_transaction tx, model::timeout_clock::duration timeout, ss::lw_shared_ptr<ss::promise<tx_errc>> outcome) {
    auto changed_tx = co_await stm->try_change_status(tx.id, tx.etag, cluster::tm_transaction::tx_status::aborting);
    if (!changed_tx.has_value()) {
        outcome->set_value(tx_errc::timeout);
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    outcome->set_value(tx_errc::none);

    tx = changed_tx.value();
    std::vector<ss::future<abort_tx_reply>> pfs;
    for (auto rm : tx.partitions) {
        pfs.push_back(_rm_partition_frontend.local().abort_tx(rm.ntp, tx.pid, timeout));
    }
    std::vector<ss::future<abort_group_tx_reply>> gfs;
    for (auto group : tx.groups) {
        gfs.push_back(_rm_group_frontend.local().abort_group_tx(group.group_id, tx.pid, timeout));
    }
    auto prs = co_await when_all_succeed(pfs.begin(), pfs.end());
    auto grs = co_await when_all_succeed(gfs.begin(), gfs.end());
    bool ok = true;
    for (auto r : prs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    for (auto r : grs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    if (!ok) {
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    changed_tx = stm->mark_tx_finished(tx.id, tx.etag);
    if (!changed_tx.has_value()) {
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    tx = changed_tx.value();
    co_return checked<cluster::tm_transaction, tx_errc>(tx);
}

ss::future<checked<cluster::tm_transaction, tx_errc>>
tx_gateway_frontend::commit_tm_tx(ss::shared_ptr<cluster::tm_stm>& stm, cluster::tm_transaction tx, model::timeout_clock::duration timeout, ss::lw_shared_ptr<ss::promise<tx_errc>> outcome) {
    return stm->get_end_lock(tx.id)->with([this, &stm, timeout, tx, outcome](){
        return do_commit_tm_tx(stm, tx, timeout, outcome);
    });
}

ss::future<checked<cluster::tm_transaction, tx_errc>>
tx_gateway_frontend::do_commit_tm_tx(ss::shared_ptr<cluster::tm_stm>& stm, cluster::tm_transaction tx, model::timeout_clock::duration timeout, ss::lw_shared_ptr<ss::promise<tx_errc>> outcome) {
    std::vector<ss::future<prepare_tx_reply>> pfs;
    for (auto rm : tx.partitions) {
        pfs.push_back(_rm_partition_frontend.local().prepare_tx(rm.ntp, rm.etag, model::kafka_tx_ntp.tp.partition, tx.pid, tx.tx_seq, timeout));
    }

    std::vector<ss::future<prepare_group_tx_reply>> pgfs;
    for (auto group : tx.groups) {
        pgfs.push_back(_rm_group_frontend.local().prepare_group_tx(group.group_id, group.etag, tx.pid, tx.tx_seq, timeout));
    }

    if (tx.status == tm_transaction::tx_status::ongoing) {
        auto became_preparing_tx = co_await stm->try_change_status(tx.id, tx.etag, cluster::tm_transaction::tx_status::preparing);
        if (!became_preparing_tx.has_value()) {
            outcome->set_value(tx_errc::timeout);
            co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
        }
        tx = became_preparing_tx.value();
    }
    
    bool ok = true;
    auto prs = co_await when_all_succeed(pfs.begin(), pfs.end());
    for (auto r : prs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    auto pgrs = co_await when_all_succeed(pgfs.begin(), pgfs.end());
    for (auto r : pgrs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    if (!ok) {
        outcome->set_value(tx_errc::timeout);
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    outcome->set_value(tx_errc::none);
    
    auto changed_tx = co_await stm->try_change_status(tx.id, tx.etag, cluster::tm_transaction::tx_status::prepared);
    if (!changed_tx.has_value()) {
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    tx = changed_tx.value();

    std::vector<ss::future<commit_group_tx_reply>> gfs;
    for (auto group : tx.groups) {
        gfs.push_back(_rm_group_frontend.local().commit_group_tx(group.group_id, tx.pid, tx.tx_seq, timeout));
    }
    std::vector<ss::future<commit_tx_reply>> cfs;
    for (auto rm : tx.partitions) {
        cfs.push_back(_rm_partition_frontend.local().commit_tx(rm.ntp, tx.pid, tx.tx_seq, timeout));
    }
    ok = true;
    auto grs = co_await when_all_succeed(gfs.begin(), gfs.end());
    for (auto r : grs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    auto crs = co_await when_all_succeed(cfs.begin(), cfs.end());
    for (auto r : crs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    if (!ok) {
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    changed_tx = stm->mark_tx_finished(tx.id, tx.etag);
    if (!changed_tx.has_value()) {
        co_return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
    }
    tx = changed_tx.value();
    co_return checked<cluster::tm_transaction, tx_errc>(tx);
}

ss::future<checked<tm_transaction, tx_errc>>
tx_gateway_frontend::recommit_tm_tx(ss::shared_ptr<tm_stm>& stm, tm_transaction tx, model::timeout_clock::duration timeout) {
    std::vector<ss::future<commit_group_tx_reply>> gfs;
    for (auto group : tx.groups) {
        gfs.push_back(_rm_group_frontend.local().commit_group_tx(group.group_id, tx.pid, tx.tx_seq, timeout));
    }
    std::vector<ss::future<commit_tx_reply>> cfs;
    for (auto rm : tx.partitions) {
        cfs.push_back(_rm_partition_frontend.local().commit_tx(rm.ntp, tx.pid, tx.tx_seq, timeout));
    }

    auto ok = true;
    auto grs = co_await when_all_succeed(gfs.begin(), gfs.end());
    for (auto r : grs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    auto crs = co_await when_all_succeed(cfs.begin(), cfs.end());
    for (auto r : crs) {
        ok = ok && (r.ec == tx_errc::none);
    }

    if (ok) {
        co_return checked<tm_transaction, tx_errc>(tx);
    }
    co_return checked<tm_transaction, tx_errc>(tx_errc::timeout);
}

ss::future<checked<tm_transaction, tx_errc>>
tx_gateway_frontend::reabort_tm_tx(ss::shared_ptr<tm_stm>& stm, tm_transaction tx, model::timeout_clock::duration timeout) {
    std::vector<ss::future<abort_tx_reply>> pfs;
    for (auto rm : tx.partitions) {
        pfs.push_back(_rm_partition_frontend.local().abort_tx(rm.ntp, tx.pid, timeout));
    }
    std::vector<ss::future<abort_group_tx_reply>> gfs;
    for (auto group : tx.groups) {
        gfs.push_back(_rm_group_frontend.local().abort_group_tx(group.group_id, tx.pid, timeout));
    }
    auto prs = co_await when_all_succeed(pfs.begin(), pfs.end());
    auto grs = co_await when_all_succeed(pfs.begin(), pfs.end());
    auto ok = true;
    for (auto r : prs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    for (auto r : grs) {
        ok = ok && (r.ec == tx_errc::none);
    }
    if (ok) {
        co_return checked<tm_transaction, tx_errc>(tx);
    }
    co_return checked<tm_transaction, tx_errc>(tx_errc::timeout);
}

ss::future<checked<tm_transaction, tx_errc>>
tx_gateway_frontend::get_tx(ss::shared_ptr<tm_stm>& stm, model::producer_identity pid, kafka::transactional_id transactional_id, model::timeout_clock::duration timeout) {
    auto maybe_tx = stm->get_tx(transactional_id);
    if (!maybe_tx) {
        return ss::make_ready_future<checked<tm_transaction, tx_errc>>(tx_errc::timeout);
    }


    auto tx = maybe_tx.value();

    if (tx.pid != pid) {
        return ss::make_ready_future<checked<tm_transaction, tx_errc>>(tx_errc::timeout);
    }
    
    if (tx.status == tm_transaction::tx_status::aborting) {
        return ss::make_ready_future<checked<tm_transaction, tx_errc>>(tx_errc::timeout);
    }

    auto f = ss::make_ready_future<checked<tm_transaction, tx_errc>>(tx);

    if (tx.status != tm_transaction::tx_status::ongoing) {
        if (tx.status == tm_transaction::tx_status::preparing) {
            f = commit_tm_tx(stm, tx, timeout, ss::make_lw_shared<ss::promise<tx_errc>>());
        } else if (tx.status == tm_transaction::tx_status::prepared) {
            f = recommit_tm_tx(stm, tx, timeout);
        } else {
            vassert(tx.status == tm_transaction::tx_status::finished, "unexpected tx status {}", tx.status);
        }
    
        f = f.then([&stm](checked<tm_transaction, tx_errc> r) {
            if (!r.has_value()) {
                return r;
            }
            
            auto tx = r.value();
            
            auto changed_tx = stm->mark_tx_ongoing(tx.id, tx.etag);
            if (!changed_tx.has_value()) {
                return checked<cluster::tm_transaction, tx_errc>(tx_errc::timeout);
            }
            return checked<cluster::tm_transaction, tx_errc>(changed_tx.value());
        });
    }

    return f;
}

ss::future<bool>
tx_gateway_frontend::try_create_tx_topic() {
    cluster::topic_configuration topic{
      model::kafka_internal_namespace,
      model::kafka_tx_topic,
      1,
      config::shard_local_cfg().default_topic_replication()};

    topic.properties.cleanup_policy_bitflags = model::cleanup_policy_bitflags::none;

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
          vlog(clusterlog.warn, "cant create tx id stm topic {}", e);
          return false;
      });
}

} // namespace cluster
