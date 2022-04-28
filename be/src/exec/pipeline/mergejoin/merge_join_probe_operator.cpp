// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/mergejoin/merge_join_probe_operator.h"

namespace starrocks {
namespace pipeline {

MergeJoinProbeOperator::MergeJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name,
                                             int32_t plan_node_id, MergeJoinerPtr join_prober,
                                             MergeJoinerPtr join_builder)
        : OperatorWithDependency(factory, id, name, plan_node_id),
          _join_prober(std::move(join_prober)),
          _join_builder(std::move(join_builder)) {}

void MergeJoinProbeOperator::close(RuntimeState* state) {
    _join_prober->unref(state);
    if (_join_builder != _join_prober) {
        _join_builder->unref(state);
    }

    OperatorWithDependency::close(state);
}

Status MergeJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependency::prepare(state));

    if (_join_builder != _join_prober) {//不一样的情况，是哪些情况？
        _join_builder->ref();
    }
    _join_prober->ref();

    RETURN_IF_ERROR(_join_prober->prepare_prober(state, _unique_metrics.get()));

    return Status::OK();
}

bool MergeJoinProbeOperator::has_output() const {
    return _join_prober->has_output();
}

bool MergeJoinProbeOperator::need_input() const {
    return _join_prober->need_input();
}

bool MergeJoinProbeOperator::is_finished() const {
    return _join_prober->is_done();//对应到eos阶段了
}


Status MergeJoinProbeOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _join_prober->push_chunk(state, std::move(const_cast<vectorized::ChunkPtr&>(chunk)));
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> MergeJoinProbeOperator::pull_chunk(RuntimeState* state) {
    return _join_prober->pull_chunk(state);
}

void MergeJoinProbeOperator::set_finishing(RuntimeState* state) {
    //此处需要进行左右表对齐操作。
    _is_finished = true;
    _join_prober->enter_post_probe_phase();
}

void MergeJoinProbeOperator::set_finished(RuntimeState* state) {
    _join_prober->enter_eos_phase();
    _join_builder->set_prober_finished();//这步有点迷
}

bool MergeJoinProbeOperator::is_ready() const {
    return _join_prober->is_build_done();
}

MergeJoinProbeOperatorFactory::MergeJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id,
                                                           MergeJoinerFactoryPtr merge_joiner_factory)
        : OperatorFactory(id, "merge_join_probe", plan_node_id), _merge_joiner_factory(std::move(merge_joiner_factory)) {}

Status MergeJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    return OperatorFactory::prepare(state);
}
void MergeJoinProbeOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

OperatorPtr MergeJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<MergeJoinProbeOperator>(this, _id, _name, _plan_node_id,
                                                   _merge_joiner_factory->create_prober(driver_sequence),
                                                   _merge_joiner_factory->create_builder(driver_sequence));
}

}
}