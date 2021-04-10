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
#include "cluster/fwd.h"
#include "kafka/protocol/delete_groups.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/txn_offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/server/group.h"
#include "kafka/server/group_stm.h"
#include "kafka/server/member.h"
#include "model/namespace.h"
#include "raft/group_manager.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/coroutine.hh>

#include <absl/container/node_hash_map.h>
#include <cluster/partition_manager.h>

namespace kafka {

struct recovery_batch_consumer_state;

/*
 * \brief Manages the Kafka group lifecycle.
 *
 * Dynamic partition attachment
 * ============================
 *
 * When a partition belonging to the internal group metadata topic comes under
 * management on a node it is dynamically registered with the group manager. The
 * set of partitions is managed in the group manager in an index:
 *
 *     [ntp -> attached_partition]
 *
 * Where `attached_partition` is a struct with an initial state of:
 *
 *     { loading = true, semaphore(1), abort_source, partition }
 *
 * Leadership changes
 * ==================
 *
 * The group manager handles leadership changes by transitioning the state of
 * attached partitions, either by recovering state or clearing state from cache.
 *
 * On leadership change notification
 *
 *     1. Do nothing if notification is for a non-registered partition
 *     2. While holding the attached partition's semaphore
 *     3. Recover or unload (see below)
 *
 * Leadership change notifications current occur for _all_ partitions on the
 * current {node, core} pair (not just group metadata partitions). Hence, we do
 * nothing for non-registered partitions. See dynamic partition attachment
 * discussion above regarding registration.
 *
 * In order to support (1) parallel partition recovery (important during
 * start-up) and (2) flapping leadership we grab a per-partition semaphore to
 * queue state transitions on the registered partition.
 *
 * The semaphore is used to serialize recovery/unload requests which are
 * themselves an asynchronous fiber. The controller/raft dispatches these
 * requests as leadership changes, and the requests are sync upcalls that can't
 * be handled synchronously without blocking raft.
 *
 * When a new upcall is received, we use the abort source to request that any
 * on-going recovery/unload is stopped promptly.
 *
 * Recovery (background)
 * =====================
 *
 * - Both recovery and partition unload are serialized per-partition
 * - Recovery occurs when the local node is leader, else unload (below)
 *
 * The recovery process reads the entire log and deduplicates entries into the
 * `recovery_batch_consumer` object.
 *
 * After the log is read the deduplicated state is used to re-populate the
 * in-memory cache of groups/commits through.
 *
 * Unload (background)
 * ===================
 *
 * - Both recovery and partition unload are serialized per-partition
 * - Unloading occurs when the local node loses partition leadership
 *
 * This process involves identifying the groups/commits that map to a partition
 * for which the local node is no longer a leader. The in-memory cache will be
 * cleared.
 *
 *     - This is not yet implemented.
 */
class group_manager {
public:
    group_manager(
      ss::sharded<raft::group_manager>& gm,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::topic_table>&,
      config::configuration& conf);

    ss::future<> start();
    ss::future<> stop();

public:
    /// \brief Handle a JoinGroup request
    ss::future<join_group_response> join_group(join_group_request&& request);

    /// \brief Handle a SyncGroup request
    ss::future<sync_group_response> sync_group(sync_group_request&& request);

    /// \brief Handle a Heartbeat request
    ss::future<heartbeat_response> heartbeat(heartbeat_request&& request);

    /// \brief Handle a LeaveGroup request
    ss::future<leave_group_response> leave_group(leave_group_request&& request);

    /// \brief Handle a OffsetCommit request
    ss::future<offset_commit_response>
    offset_commit(offset_commit_request&& request);

    ss::future<txn_offset_commit_response>
    txn_offset_commit(txn_offset_commit_request&& request);

    ss::future<mark_group_committed_result>
    mark_group_committed(mark_group_committed_request&& request);

    ss::future<cluster::begin_group_tx_reply>
    begin_tx(cluster::begin_group_tx_request&&);

    ss::future<cluster::prepare_group_tx_reply>
    prepare_tx(cluster::prepare_group_tx_request&&);

    ss::future<cluster::abort_group_tx_reply>
    abort_tx(cluster::abort_group_tx_request&&);

    /// \brief Handle a OffsetFetch request
    ss::future<offset_fetch_response>
    offset_fetch(offset_fetch_request&& request);

    // returns the set of registered groups, and an error if one occurred while
    // retrieving the group list (e.g. coordinator_load_in_progress).
    std::pair<error_code, std::vector<listed_group>> list_groups() const;

    described_group describe_group(const model::ntp&, const kafka::group_id&);

    ss::future<std::vector<deletable_group_result>>
      delete_groups(std::vector<std::pair<model::ntp, group_id>>);

public:
    error_code validate_group_status(
      const model::ntp& ntp, const group_id& group, api_key api);

    static bool valid_group_id(const group_id& group, api_key api);

private:
    group_ptr get_group(const group_id& group) {
        if (auto it = _groups.find(group); it != _groups.end()) {
            return it->second;
        }
        return nullptr;
    }

    cluster::notification_id_type _manage_notify_handle;
    ss::gate _gate;

    void attach_partition(ss::lw_shared_ptr<cluster::partition>);

    struct attached_partition {
        bool loading;
        ss::semaphore sem{1};
        ss::abort_source as;
        ss::lw_shared_ptr<cluster::partition> partition;

        explicit attached_partition(ss::lw_shared_ptr<cluster::partition> p)
          : loading(true)
          , partition(std::move(p)) {}
    };

    struct read_write_lw_lock {
        int16_t num_readers {0};
        int16_t num_writers {0};
        std::stack<ss::lw_shared_ptr<ss::promise<>>> pending_writers;

        ss::future<> take_writer_lock() {
            num_writers++;
            if (num_readers==0) {
                return ss::make_ready_future<>();
            };
            auto when_taken = ss::make_lw_shared<ss::promise<>>();
            pending_writers.push(when_taken);
            return when_taken->get_future();
        }

        void release_writer_lock() {
            num_writers--;
            if (pending_writers.size() > 0) {
                auto pending_writer = pending_writers.top();
                pending_writers.pop();
                pending_writer->set_value();
            }
        }
        
        bool take_reader_lock() {
            if (num_writers > 0) {
                return false;
            }
            num_readers++;
            return true;
        }

        void release_reader_lock() {
            num_readers--;
            if (num_readers == 0 && pending_writers.size() > 0) {
                auto pending_writer = pending_writers.top();
                pending_writers.pop();
                pending_writer->set_value();
            }
        }
    };

    absl::node_hash_map<model::ntp, ss::lw_shared_ptr<attached_partition>>
      _partitions;

    cluster::notification_id_type _leader_notify_handle;
    cluster::notification_id_type _topic_table_notify_handle;

    void handle_leader_change(
      model::term_id, ss::lw_shared_ptr<cluster::partition>, std::optional<model::node_id>);

    void handle_topic_delta(const std::vector<cluster::topic_table_delta>&);

    ss::future<> cleanup_removed_topic_partitions(
      const std::vector<model::topic_partition>&);

    ss::future<> handle_partition_leader_change(
      model::term_id,
      ss::lw_shared_ptr<attached_partition>,
      std::optional<model::node_id> leader_id);

    ss::future<> recover_partition(
      model::term_id, ss ::lw_shared_ptr<cluster::partition>, recovery_batch_consumer_state);

    ss::future<> inject_noop(
      ss::lw_shared_ptr<cluster::partition> p,
      ss::lowres_clock::time_point timeout);

    ss::sharded<raft::group_manager>& _gm;
    ss::sharded<cluster::partition_manager>& _pm;
    ss::sharded<cluster::topic_table>& _topic_table;
    config::configuration& _conf;
    absl::node_hash_map<group_id, group_ptr> _groups;
    model::broker _self;
    read_write_lw_lock _catchup_lock;
};

/**
 * the key type for group membership log records.
 *
 * the opaque key field is decoded based on the actual type.
 *
 * TODO: The `noop` type indicates a control structure used to synchronize raft
 * state in a transition to leader state so that a consistent read is made. this
 * is a temporary work-around until we fully address consistency semantics in
 * raft.
 */
struct group_log_record_key {
    enum class type : int8_t { group_metadata, offset_commit, noop };

    type record_type;
    iobuf key;
};

/*
 * This batch consumer is used during partition recovery to read, index, and
 * deduplicate both group and commit metadata snapshots.
 */
struct recovery_batch_consumer_state {
    absl::node_hash_map<kafka::group_id, group_stm> groups;
};

struct recovery_batch_consumer {
    explicit recovery_batch_consumer(ss::abort_source& as)
      : as(as) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch);

    ss::future<> handle_record(model::record);
    ss::future<> handle_group_metadata(iobuf, std::optional<iobuf>);
    ss::future<> handle_offset_metadata(iobuf, std::optional<iobuf>);

    recovery_batch_consumer_state end_of_stream() { return std::move(st); }

    recovery_batch_consumer_state st;
    model::offset batch_base_offset;

    ss::abort_source& as;
};

} // namespace kafka
