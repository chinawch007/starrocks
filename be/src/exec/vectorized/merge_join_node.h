// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/fixed_length_column.h"//这个为什么要单拿出来。
#include "exec/exec_node.h"
#include "exec/vectorized/join_hash_map.h"
#include "util/phmap/phmap.h"//这是个什么map？

namespace starrocks {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class ExprContext;

namespace vectorized {
class ColumnRef;
class RuntimeFilterBuildDescriptor;

struct MergeTableSlotDescriptor {//joiner也会用，放这里合适吗？
    SlotDescriptor* slot;
    bool need_output;
};

class MergeJoinNode final : public ExecNode {

public:
    MergeJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~MergeJoinNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    Status init(const TPlanNode& tnode, RuntimeState* state);
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;
    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;
    void Merge(ChunkPtr* chunk);
private:
    Status _build(RuntimeState* state);
    Status _probe(RuntimeState* state, ScopedTimer<MonotonicStopWatch>& probe_timer, ChunkPtr* chunk, bool& eos);
    //Status _probe_remain(ChunkPtr* chunk, bool& eos);

    ChunkPtr _right_chunk;//看看怎么初始化下 
    ChunkPtr _left_chunk;
    ChunkPtr _result_chunk;

    friend ExecNode;
    // _hash_join_node is used to construct HashJoiner, the reference is sound since
    // it's only used in FragmentExecutor::prepare function.
    const TMergeJoinNode& _merge_join_node;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;
    //std::vector<ExprContext*> _other_join_conjunct_ctxs;
    //std::vector<bool> _is_null_safes;

    //std::vector<ExprContext*> _probe_equivalence_partition_expr_ctxs;
    //std::vector<ExprContext*> _build_equivalence_partition_expr_ctxs;

    TJoinOp::type _join_type = TJoinOp::INNER_JOIN;
    TJoinDistributionMode::type _distribution_mode = TJoinDistributionMode::NONE;
    std::set<SlotId> _output_slots;
    bool _eos = false;

    std::vector<ExprContext*> _probe_equivalence_partition_expr_ctxs;
    std::vector<ExprContext*> _build_equivalence_partition_expr_ctxs;

    Buffer<MergeTableSlotDescriptor> right_slots;
    Buffer<MergeTableSlotDescriptor> left_slots;
};


}

}