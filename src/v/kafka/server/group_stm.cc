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
