// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/mergejoin/merge_join_build_operator.h"

#include "runtime/runtime_filter_worker.h"

namespace starrocks {
namespace pipeline {
MergeJoinBuildOperator::MergeJoinBuildOperator(OperatorFactory* factory, int32_t id, const string& name,
                                             int32_t plan_node_id, MergeJoinerPtr join_builder,
                                             size_t driver_sequence)
        : Operator(factory, id, name, plan_node_id, driver_sequence),
          _join_builder(std::move(join_builder)) {}

Status MergeJoinBuildOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    LOG(WARNING) << "buildop push chunk";
    return _join_builder->append_chunk_to_buffer(state, chunk);
}

Status MergeJoinBuildOperator::prepare(RuntimeState* state) {//state还需要我传给joiner吗？
    LOG(WARNING) << "buildop prepare in";
    RETURN_IF_ERROR(Operator::prepare(state));

    _join_builder->ref();
    //二参是个啥
    RETURN_IF_ERROR(_join_builder->prepare_builder(state, _unique_metrics.get()));

    LOG(WARNING) << "buildop prepare out";
    return Status::OK();
}

void MergeJoinBuildOperator::close(RuntimeState* state) {
    LOG(WARNING) << "buildop close in";
    _join_builder->unref(state);

    Operator::close(state);
    LOG(WARNING) << "buildop close out";
}

StatusOr<vectorized::ChunkPtr> MergeJoinBuildOperator::pull_chunk(RuntimeState* state) {
    const char* msg = "pull_chunk not supported in MergeJoinBuildOperator";
    CHECK(false) << msg;
    return Status::NotSupported(msg);
}

Status MergeJoinBuildOperator::set_finishing(RuntimeState* state) {
    LOG(WARNING) << "buildop set_finishing";
    _join_builder->enter_probe_phase();//是说build阶段收集排序，probe阶段对齐？
    return Status::OK();
}

MergeJoinBuildOperatorFactory::MergeJoinBuildOperatorFactory(
        int32_t id, int32_t plan_node_id, MergeJoinerFactoryPtr merge_joiner_factory)
        : OperatorFactory(id, "mereg_join_build", plan_node_id),
          _merge_joiner_factory(std::move(merge_joiner_factory)){LOG(WARNING) << "buildop factory construct";}

Status MergeJoinBuildOperatorFactory::prepare(RuntimeState* state) {//从这里调用的场景是什么？
    LOG(WARNING) << "buildop factory prepare";
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return _merge_joiner_factory->prepare(state);//生疏
}

void MergeJoinBuildOperatorFactory::close(RuntimeState* state) {
    LOG(WARNING) << "buildop factory close";
    _merge_joiner_factory->close(state);
    OperatorFactory::close(state);
}

OperatorPtr MergeJoinBuildOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    LOG(WARNING) << "buildop factory create";
    return std::make_shared<MergeJoinBuildOperator>(this, _id, _name, _plan_node_id,//name的话是这里的初始参数
                                                   _merge_joiner_factory->create_builder(driver_sequence),
                                                   driver_sequence);
}

}
}