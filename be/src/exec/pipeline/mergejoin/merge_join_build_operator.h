// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <exprs/predicate.h>

#include <atomic>

#include "exec/pipeline/mergejoin/merge_joiner_factory.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/vectorized/merge_joiner.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"

namespace starrocks {
namespace pipeline {

using MergeJoiner = starrocks::vectorized::MergeJoiner;

class MergeJoinBuildOperator final : public Operator {
public:
    MergeJoinBuildOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                          MergeJoinerPtr join_builder,
                          size_t driver_sequence);//这个参数是由pipeline构建方传来的应该是去不掉的。
    ~MergeJoinBuildOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override {
        CHECK(false) << "has_output not supported in MergeJoinBuildOperator";
        return false;
    }
    //这里其实有点矛盾，其实到了probe阶段就不会再接收数据了，我只能任务这里再调用过set_finishing之后就不会再调这个need_input了
    bool need_input() const override { return !is_finished(); }

    //set_finished用父类的函数了
    void set_finishing(RuntimeState* state) override;
    //我怀疑这里是轮询，并且是在joiner那边自我finished之后，所以
    bool is_finished() const override { return _join_builder->is_finished(); }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    std::string get_name() const override {
        return strings::Substitute("$0(MergeJoiner=$1)", Operator::get_name(), _join_builder.get());
    }

private:
    MergeJoinerPtr _join_builder;
    size_t _driver_sequence;
};

//这里的参数肯定是和上边的保持一致的。
class MergeJoinBuildOperatorFactory final : public OperatorFactory {
public://看看实际生成op的参数，多余的是怎么得来的
    MergeJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id, MergeJoinerFactoryPtr merge_joiner_factory);
    ~MergeJoinBuildOperatorFactory() override = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    MergeJoinerFactoryPtr _merge_joiner_factory;
};

} // namespace pipeline
} // namespace starrocks
