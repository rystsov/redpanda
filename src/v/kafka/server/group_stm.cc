#include "kafka/server/group_stm.h"

namespace kafka {

void
group_stm::overwrite_metadata(group_log_group_metadata&& metadata) {
    _metadata = std::move(metadata);
    _is_loaded = true;
}

void
group_stm::remove_offset(group_log_offset_key key) {
    _offsets.erase(key);
}

void
group_stm::update_offset(group_log_offset_key key, model::offset offset, group_log_offset_metadata&& meta) {
    _offsets[key] = std::make_pair(offset, std::move(meta));
}

void
group_stm::update_prepared(model::offset offset, group_log_prepared_tx val) {
    auto tx = group::group_prepared_tx {
        .pid = val.pid,
        .group_id = val.group_id
    };

    auto [prepared_it, inserted] = _prepared_txs.try_emplace(tx.pid.id, tx);
    if (!inserted && prepared_it->second.pid.epoch > tx.pid.epoch) {
        klog.warn("a logged tx {} is fenced off by prev logged tx {}", val.pid, prepared_it->second.pid);
        return;
    } else if (!inserted && prepared_it->second.pid.epoch < tx.pid.epoch) {
        klog.warn("a logged tx {} overwrites prev logged tx {}", val.pid, prepared_it->second.pid);
        prepared_it->second.pid = tx.pid;
        prepared_it->second.offsets.clear();
    }
    
    for (const auto& tx_offset : val.offsets) {
        group::offset_metadata md {
            .log_offset =offset,
            .offset = tx_offset.offset,
            .metadata = tx_offset.metadata.value_or(""),
        };
        // TODO: support leader_epoch
        prepared_it->second.offsets[tx_offset.tp] = md;
    }
}

void
group_stm::commit(model::producer_identity pid) {
    auto prepared_it = _prepared_txs.find(pid.id);
    if (prepared_it == _prepared_txs.end()) {
        klog.warn("can't find ongoing tx {}", pid);
        return;
    } else if (prepared_it->second.pid.epoch != pid.epoch) {
        klog.warn("a comitting tx {} doesn't match ongoing tx {}", pid, prepared_it->second.pid);
        return;
    }

    for (const auto& [tp, md] : prepared_it->second.offsets) {
        group_log_offset_key key{
            prepared_it->second.group_id,
            tp.topic,
            tp.partition,
        };
        
        group_log_offset_metadata val{
            .offset = md.offset,
            .leader_epoch = 0, // we never use leader_epoch down the stack
            .metadata = md.metadata
        };

        // todo: check log_offset to avoid overwrite
        _offsets[key] = std::make_pair(md.log_offset, std::move(val));
    }

    _prepared_txs.erase(prepared_it);
}

std::ostream& operator<<(std::ostream& os, const group_log_offset_key& key) {
    fmt::print(
      os,
      "group {} topic {} partition {}",
      key.group(),
      key.topic(),
      key.partition());
    return os;
}

std::ostream&
operator<<(std::ostream& os, const group_log_offset_metadata& md) {
    fmt::print(os, "offset {}", md.offset());
    return os;
}

}
