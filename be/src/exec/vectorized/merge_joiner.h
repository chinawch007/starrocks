// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/statusor.h"
#include "exec/exec_node.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/vectorized/merge_join_node.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "util/phmap/phmap.h"

namespace starrocks {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class ExprContext;

namespace vectorized {
class ColumnRef;
class RuntimeFilterBuildDescriptor;

class MergeJoiner;
using MergeJoinerPtr = std::shared_ptr<MergeJoiner>;

enum MergeJoinPhase {
    BUILD = 0,
    PROBE = 1,
    POST_PROBE = 2,
    EOS = 4,
};

struct MergeTableSlotDescriptor {//这部分原来在hash表中的功能，放到joiner中也算是符合原义
    SlotDescriptor* slot;
    bool need_output;
};

struct MergeJoinerParam {//看看具体哪些是用的到的。
    MergeJoinerParam(ObjectPool* pool, const TMergeJoinNode& merge_join_node, TPlanNodeId node_id,
                    TPlanNodeType::type node_type, const std::vector<bool>& is_null_safes,
                    const std::vector<ExprContext*>& build_expr_ctxs, const std::vector<ExprContext*>& probe_expr_ctxs,
                    const std::vector<ExprContext*>& other_join_conjunct_ctxs,
                    const std::vector<ExprContext*>& conjunct_ctxs, 
                    const RowDescriptor& right_row_descriptor,
                    const RowDescriptor& left_row_descriptor, 
                    const RowDescriptor& row_descriptor,
                    TPlanNodeType::type build_node_type, 
                    TPlanNodeType::type probe_node_type,
                    const std::set<SlotId>& output_slots, const TJoinDistributionMode::type distribution_mode)
            : _pool(pool),
              _merge_join_node(hash_join_node),
              _node_id(node_id),
              _node_type(node_type),
              _is_null_safes(is_null_safes),
              _build_expr_ctxs(build_expr_ctxs),
              _probe_expr_ctxs(probe_expr_ctxs),
              _other_join_conjunct_ctxs(other_join_conjunct_ctxs),
              _conjunct_ctxs(conjunct_ctxs),
              _right_row_descriptor(right_row_descriptor),
              _left_row_descriptor(left_row_descriptor),
              _row_descriptor(row_descriptor),
              _build_node_type(build_node_type),
              _probe_node_type(probe_node_type),
              _output_slots(output_slots),
              _distribution_mode(distribution_mode) {}

    MergeJoinerParam(MergeJoinerParam&&) = default;
    MergeJoinerParam(MergeJoinerParam&) = default;
    ~MergeJoinerParam() = default;

    //暂时先过一些这些参数大致的作用，做下记录。
    ObjectPool* _pool;
    const TMergeJoinNode& _merge_join_node;
    TPlanNodeId _node_id;
    TPlanNodeType::type _node_type;
    const std::vector<bool> _is_null_safes;
    const std::vector<ExprContext*> _build_expr_ctxs;
    const std::vector<ExprContext*> _probe_expr_ctxs;
    const std::vector<ExprContext*> _other_join_conjunct_ctxs;
    const std::vector<ExprContext*> _conjunct_ctxs;
    const RowDescriptor _right_row_descriptor;
    const RowDescriptor _left_row_descriptor;
    const RowDescriptor _row_descriptor;
    TPlanNodeType::type _build_node_type;
    TPlanNodeType::type _probe_node_type;
    std::set<SlotId> _output_slots;

    const TJoinDistributionMode::type _distribution_mode;
};

class MergeJoiner final : public pipeline::ContextWithDependency {
public:
    //我要不要后边的prober还不一定。注意此处用到了其他的MergeJoiner
    explicit MergeJoiner(const MergeJoinerParam& param);

    ~MergeJoiner() {
        if (_runtime_state != nullptr) {
            close(_runtime_state);
        }
    }

    Status prepare_builder(RuntimeState* state, RuntimeProfile* runtime_profile);
    Status prepare_prober(RuntimeState* state, RuntimeProfile* runtime_profile);
    void close(RuntimeState* state) override;

    bool need_input() const;
    bool has_output() const;
    bool is_build_done() const { return _phase != MergeJoinPhase::BUILD; }
    bool is_done() const { return _phase == MergeJoinPhase::EOS; }

    void enter_probe_phase() {
        auto old_phase = MergeJoinPhase::BUILD;
        _phase.compare_exchange_strong(old_phase, MergeJoinPhase::PROBE);
    }
    void enter_post_probe_phase() {
        MergeJoinPhase old_phase = MergeJoinPhase::PROBE;
        if (!_phase.compare_exchange_strong(old_phase, MergeJoinPhase::POST_PROBE)) {
            old_phase = MergeJoinPhase::BUILD;
            // HashJoinProbeOperator finishes prematurely on runtime error or fragment's cancellation.
            _phase.compare_exchange_strong(old_phase, MergeJoinPhase::EOS);
        }
    }
    void enter_eos_phase() { _phase = MergeJoinPhase::EOS; }//没线程间竞争了吗？
    //自己改的喽
    Status append_chunk_to_buffer(RuntimeState* state, const ChunkPtr& chunk);
    Status sort_buffer(RuntimeState* state);

    void push_chunk(RuntimeState* state, ChunkPtr&& chunk);
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state);

    size_t get_row_count() { return _ht.get_row_count(); }

    // These two methods are used only by the hash join builder.
    void set_builder_finished();
    void set_prober_finished();

private:
    static bool _has_null(const ColumnPtr& column);

    //Status _build(RuntimeState* state);
    //Status _probe(RuntimeState* state, ScopedTimer<MonotonicStopWatch>& probe_timer, ChunkPtr* chunk, bool& eos);

    StatusOr<ChunkPtr> _pull_probe_output_chunk(RuntimeState* state);

private:
    const TMergeJoinNode& _merge_join_node;
    ObjectPool* _pool;

    RuntimeState* _runtime_state = nullptr;

    TJoinOp::type _join_type = TJoinOp::INNER_JOIN;
    std::atomic<MergeJoinPhase> _phase = MergeJoinPhase::BUILD;
    bool _is_closed = false;

    //ChunkPtr _probe_input_chunk;
    ChunkPtr _right_chunk;
    ChunkPtr _left_chunk;
    // Equal conjuncts in Join On.
    const std::vector<ExprContext*>& _expr_ctxs;//我理解这就是表关联等式。我理解他从chunk获取相应的列。

    //fiter相关5个

    const RowDescriptor& _right_row_descriptor;
    const RowDescriptor& _left_row_descriptor;
    const RowDescriptor& _row_descriptor;
    const TPlanNodeType::type _build_node_type;
    const TPlanNodeType::type _probe_node_type;
    const bool _build_conjunct_ctxs_is_empty;
    const std::set<SlotId>& _output_slots;

    //filter相关4个

    bool _is_push_down = false;

    //Columns _key_columns;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;
    
    const std::vector<HashJoinerPtr>& _read_only_join_probers;
    std::atomic<size_t> _num_unfinished_probers = 0;

    //一堆counter

    //自己新增，从ht中取出的
    Buffer<MergeTableSlotDescriptor> right_slots;
    Buffer<MergeTableSlotDescriptor> left_slots;
}

}
}