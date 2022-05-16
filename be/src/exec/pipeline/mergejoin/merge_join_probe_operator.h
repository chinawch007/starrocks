// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/mergejoin/merge_joiner_factory.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/vectorized/merge_joiner.h"

namespace starrocks {
namespace pipeline {

using MergeJoiner = starrocks::vectorized::MergeJoiner;

class MergeJoinProbeOperator final : public OperatorWithDependency {//跟builder父类不一样。
public:
    MergeJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                          MergeJoinerPtr join_prober);
    ~MergeJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool need_input() const override;

    bool is_finished() const override;
    void set_finishing(RuntimeState* state) override;
    void set_finished(RuntimeState* state) override;

    bool is_ready() const override;
    std::string get_name() const override {
        return strings::Substitute("$0(MergeJoiner=$1)", Operator::get_name(), _join_prober.get());
    }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk);
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state);

private:
    const MergeJoinerPtr _join_prober;
};

class MergeJoinProbeOperatorFactory final : public OperatorFactory {
public:
    MergeJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, MergeJoinerFactoryPtr merge_joiner);

    ~MergeJoinProbeOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    MergeJoinerFactoryPtr _merge_joiner_factory;
};



}
}