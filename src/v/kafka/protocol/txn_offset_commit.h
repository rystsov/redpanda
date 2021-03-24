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

#include "bytes/iobuf.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/txn_offset_commit_request.h"
#include "kafka/protocol/schemata/txn_offset_commit_response.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct txn_offset_commit_response;

struct txn_offset_commit_api final {
    using response_type = txn_offset_commit_response;

    static constexpr const char* name = "txn offset commit";
    static constexpr api_key key = api_key(28);
};

struct txn_offset_commit_request final {
    using api_type = txn_offset_commit_api;

    txn_offset_commit_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const txn_offset_commit_request& r) {
    return os << r.data;
}

struct txn_offset_commit_response final {
    using api_type = txn_offset_commit_api;

    txn_offset_commit_response_data data;

    void encode(const request_context& ctx, response& resp) {
        data.encode(resp.writer(), ctx.header().version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const txn_offset_commit_response& r) {
    return os << r.data;
}

} // namespace kafka
