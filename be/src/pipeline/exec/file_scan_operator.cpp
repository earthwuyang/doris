// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "pipeline/exec/file_scan_operator.h"

#include <fmt/format.h>

#include <memory>

#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/exec/scan_operator.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/scan/vfile_scanner.h"

namespace doris::pipeline {

Status FileScanLocalState::_init_scanners(std::list<vectorized::VScannerSPtr>* scanners) {
    if (_scan_ranges.empty()) {
        _eos = true;
        _scan_dependency->set_ready();
        return Status::OK();
    }

    auto& p = _parent->cast<FileScanOperatorX>();
    size_t shard_num = std::min<size_t>(
            config::doris_scanner_thread_pool_thread_num / state()->query_parallel_instance_num(),
            _scan_ranges.size());
    shard_num = std::max(shard_num, (size_t)1);
    _kv_cache.reset(new vectorized::ShardedKVCache(shard_num));
    for (auto& scan_range : _scan_ranges) {
        std::unique_ptr<vectorized::VFileScanner> scanner = vectorized::VFileScanner::create_unique(
                state(), this, p._limit_per_scanner,
                scan_range.scan_range.ext_scan_range.file_scan_range, _scanner_profile.get(),
                _kv_cache.get());
        RETURN_IF_ERROR(
                scanner->prepare(_conjuncts, &_colname_to_value_range, &_colname_to_slot_id));
        scanners->push_back(std::move(scanner));
    }
    return Status::OK();
}

std::string FileScanLocalState::name_suffix() const {
    return fmt::format(" (id={}. table name = {})", std::to_string(_parent->node_id()),
                       _parent->cast<FileScanOperatorX>()._table_name);
}

void FileScanLocalState::set_scan_ranges(RuntimeState* state,
                                         const std::vector<TScanRangeParams>& scan_ranges) {
    int max_scanners =
            config::doris_scanner_thread_pool_thread_num / state->query_parallel_instance_num();
    max_scanners = std::max(std::max(max_scanners, state->parallel_scan_max_scanners_count()), 1);
    // For select * from table limit 10; should just use one thread.
    if (should_run_serial()) {
        max_scanners = 1;
    }
    if (scan_ranges.size() <= max_scanners) {
        _scan_ranges = scan_ranges;
    } else {
        // There is no need for the number of scanners to exceed the number of threads in thread pool.
        // scan_ranges is sorted by path(as well as partition path) in FE, so merge scan ranges in order.
        // In the insert statement, reading data in partition order can reduce the memory usage of BE
        // and prevent the generation of smaller tables.
        _scan_ranges.resize(max_scanners);
        int num_ranges = scan_ranges.size() / max_scanners;
        int num_add_one = scan_ranges.size() - num_ranges * max_scanners;
        int scan_index = 0;
        int range_index = 0;
        for (int i = 0; i < num_add_one; ++i) {
            _scan_ranges[scan_index] = scan_ranges[range_index++];
            auto& ranges =
                    _scan_ranges[scan_index++].scan_range.ext_scan_range.file_scan_range.ranges;
            for (int j = 0; j < num_ranges; j++) {
                auto& merged_ranges =
                        scan_ranges[range_index++].scan_range.ext_scan_range.file_scan_range.ranges;
                ranges.insert(ranges.end(), merged_ranges.begin(), merged_ranges.end());
            }
        }
        for (int i = num_add_one; i < max_scanners; ++i) {
            _scan_ranges[scan_index] = scan_ranges[range_index++];
            auto& ranges =
                    _scan_ranges[scan_index++].scan_range.ext_scan_range.file_scan_range.ranges;
            for (int j = 0; j < num_ranges - 1; j++) {
                auto& merged_ranges =
                        scan_ranges[range_index++].scan_range.ext_scan_range.file_scan_range.ranges;
                ranges.insert(ranges.end(), merged_ranges.begin(), merged_ranges.end());
            }
        }
        LOG(INFO) << "Merge " << scan_ranges.size() << " scan ranges to " << _scan_ranges.size();
    }
    if (scan_ranges.size() > 0 &&
        scan_ranges[0].scan_range.ext_scan_range.file_scan_range.__isset.params) {
        // for compatibility.
        // in new implement, the tuple id is set in prepare phase
        _output_tuple_id =
                scan_ranges[0].scan_range.ext_scan_range.file_scan_range.params.dest_tuple_id;
    }
}

Status FileScanLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(ScanLocalState<FileScanLocalState>::init(state, info));
    auto& p = _parent->cast<FileScanOperatorX>();
    _output_tuple_id = p._output_tuple_id;
    return Status::OK();
}

Status FileScanLocalState::_process_conjuncts() {
    RETURN_IF_ERROR(ScanLocalState<FileScanLocalState>::_process_conjuncts());
    if (Base::_eos) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
    return Status::OK();
}

Status FileScanOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanOperatorX<FileScanLocalState>::prepare(state));
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.contains(node_id())) {
        TFileScanRangeParams& params =
                state->get_query_ctx()->file_scan_range_params_map[node_id()];
        _output_tuple_id = params.dest_tuple_id;
    }
    return Status::OK();
}

} // namespace doris::pipeline
