// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/mergejoin/merge_join_probe_operator.h"

namespace starrocks {
namespace pipeline {

MergeJoinProbeOperator::MergeJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name,
                                             int32_t plan_node_id, int32_t driver_sequence, MergeJoinerPtr join_prober)
        : OperatorWithDependency(factory, id, name, plan_node_id, driver_sequence),
          _join_prober(std::move(join_prober)) {LOG(WARNING) << "probeop construct";}

void MergeJoinProbeOperator::close(RuntimeState* state) {
    LOG(WARNING) << "probeop close";
    _join_prober->unref(state);

    OperatorWithDependency::close(state);
}

Status MergeJoinProbeOperator::prepare(RuntimeState* state) {
    LOG(WARNING) << "probeop prepare in";
    RETURN_IF_ERROR(OperatorWithDependency::prepare(state));

    _join_prober->ref();

    RETURN_IF_ERROR(_join_prober->prepare_prober(state, _unique_metrics.get()));

    LOG(WARNING) << "probeop prepare out";
    return Status::OK();
}

bool MergeJoinProbeOperator::has_output() const {
    LOG(WARNING) << "probeop has_output";
    return _join_prober->has_output();
}

bool MergeJoinProbeOperator::need_input() const {
    LOG(WARNING) << "probeop need_input";
    return _join_prober->need_input();//跟上边都一样，直接都同名函数了？这么奢侈？
}

bool MergeJoinProbeOperator::is_finished() const {
    LOG(WARNING) << "probeop is_finished";
    return _join_prober->is_done();//对应到eos阶段了
}


Status MergeJoinProbeOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    LOG(WARNING) << "probeop push_chunk";
    _join_prober->push_chunk(state, std::move(const_cast<vectorized::ChunkPtr&>(chunk)));
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> MergeJoinProbeOperator::pull_chunk(RuntimeState* state) {
    LOG(WARNING) << "probeop pull_chunk";
    return _join_prober->pull_chunk(state);
}

Status MergeJoinProbeOperator::set_finishing(RuntimeState* state) {
    //此处需要进行左右表对齐操作。
    LOG(WARNING) << "probeop set_finishing";
    _join_prober->enter_post_probe_phase();
    return Status::OK();
}

Status MergeJoinProbeOperator::set_finished(RuntimeState* state) {//理论上是has_output拿不到数据之后才调这个的
    LOG(WARNING) << "probeop set_finished";
    _join_prober->enter_eos_phase();
    return Status::OK();
}

bool MergeJoinProbeOperator::is_ready() const {
    LOG(WARNING) << "probeop is_ready";
    return _join_prober->is_build_done();
}

MergeJoinProbeOperatorFactory::MergeJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id,
                                                           MergeJoinerFactoryPtr merge_joiner_factory)
        : OperatorFactory(id, "merge_join_probe", plan_node_id), _merge_joiner_factory(std::move(merge_joiner_factory)) {LOG(WARNING) << "probeop factory construct";}

Status MergeJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    LOG(WARNING) << "probeop factory prepare";
    return OperatorFactory::prepare(state);
}
void MergeJoinProbeOperatorFactory::close(RuntimeState* state) {
    LOG(WARNING) << "probeop factory close";
    OperatorFactory::close(state);
}

OperatorPtr MergeJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    LOG(WARNING) << "probeop factory create";
    return std::make_shared<MergeJoinProbeOperator>(this, _id, _name, _plan_node_id,
                                                   driver_sequence, _merge_joiner_factory->create_prober(driver_sequence));
}

}
}