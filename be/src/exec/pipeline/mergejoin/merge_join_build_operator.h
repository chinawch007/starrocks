// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <exprs/predicate.h>

#include <atomic>

#include "exec/pipeline/mergejoin/merge_joiner_factory.h"//这文件你还没写呢
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
    bool need_input() const override { return !is_finished(); }

    //为啥没有set_finished
    void set_finishing(RuntimeState* state) override;
    bool is_finished() const override { return _is_finished || _join_builder->is_finished(); }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    std::string get_name() const override {
        return strings::Substitute("$0(MergeJoiner=$1)", Operator::get_name(), _join_builder.get());
    }

private:
    MergeJoinerPtr _join_builder;
    // Assign the readable hash table from _join_builder to each only probe hash_joiner,
    // when _join_builder finish building the hash tbale.
    bool _is_finished = false;
};

class MergeJoinBuildOperatorFactory final : public OperatorFactory {
public:
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
