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

class MergeJoinNode final : public ExecNode {

public:
    MergeJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~MergeJoinNode() override {
        if (runtime_state() != nullptr) {
            close(runtime_state());
        }
    }

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;
private:
    friend ExecNode;
    // _hash_join_node is used to construct HashJoiner, the reference is sound since
    // it's only used in FragmentExecutor::prepare function.
    const TMergeJoinNode& _hash_join_node;

}


}

}