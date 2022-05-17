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

struct MergeJoinerParam {//看看具体哪些是用的到的。
    MergeJoinerParam(ObjectPool* pool, const TMergeJoinNode& merge_join_node, TPlanNodeId node_id,
                    const std::vector<ExprContext*>& build_expr_ctxs, const std::vector<ExprContext*>& probe_expr_ctxs,
                    const RowDescriptor& right_row_descriptor,
                    const RowDescriptor& left_row_descriptor, 
                    const std::set<SlotId>& output_slots)
            : _pool(pool),
              _merge_join_node(merge_join_node),
              _node_id(node_id),
              _build_expr_ctxs(build_expr_ctxs),
              _probe_expr_ctxs(probe_expr_ctxs),
              _right_row_descriptor(right_row_descriptor),
              _left_row_descriptor(left_row_descriptor),
              _output_slots(output_slots) {}

    MergeJoinerParam(MergeJoinerParam&&) = default;
    MergeJoinerParam(MergeJoinerParam&) = default;
    ~MergeJoinerParam() = default;

    ObjectPool* _pool;
    const TMergeJoinNode& _merge_join_node;
    TPlanNodeId _node_id;
    const std::vector<ExprContext*> _build_expr_ctxs;
    const std::vector<ExprContext*> _probe_expr_ctxs;
    const RowDescriptor _right_row_descriptor;
    const RowDescriptor _left_row_descriptor;
    std::set<SlotId> _output_slots;
};

class MergeJoiner final : public pipeline::ContextWithDependency {
public:
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
    bool is_build_done() const { return _phase != MergeJoinPhase::BUILD; }//既然这里是被询问，说明是不受probe端控制的
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
        Merge(&_result_chunk);//这俩谁先谁后呢？
    }
    void enter_eos_phase() { 
        _phase = MergeJoinPhase::EOS; 
        set_finished();//也不知道这样合不合理，但这样build和probe就都可以停止了。
    }
    Status append_chunk_to_buffer(RuntimeState* state, const ChunkPtr& chunk);
    //Status sort_buffer(RuntimeState* state);
    void Merge(ChunkPtr* chunk);//you should use the correct way to handle ptr

    void push_chunk(RuntimeState* state, ChunkPtr&& chunk);
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state);
    
private:
    const TMergeJoinNode& _merge_join_node;
    ObjectPool* _pool;

    RuntimeState* _runtime_state = nullptr;

    std::atomic<MergeJoinPhase> _phase = MergeJoinPhase::BUILD;

    //ChunkPtr _probe_input_chunk;
    ChunkPtr _right_chunk;
    ChunkPtr _left_chunk;
    ChunkPtr _result_chunk;
    // Equal conjuncts in Join On.这地方编译没过，一会看一下这个功能，然后相应的怎么初始化。
    //const std::vector<ExprContext*>& _expr_ctxs;//我理解这就是表关联等式。我理解他从chunk获取相应的列。
    const std::vector<ExprContext*>& _probe_expr_ctxs;//这里是引用，node那里是本体
    const std::vector<ExprContext*>& _build_expr_ctxs;

    //fiter相关5个

    const RowDescriptor& _right_row_descriptor;//父类直接用child赋值的，所以父类不用有相应成员。
    const RowDescriptor& _left_row_descriptor;
    const std::set<SlotId>& _output_slots;

    //filter相关4个
    //一堆counter

    Buffer<MergeTableSlotDescriptor> right_slots;
    Buffer<MergeTableSlotDescriptor> left_slots;
};

}
}