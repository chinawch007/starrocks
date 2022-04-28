// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/merge_join_node.h"

#include <runtime/runtime_state.h>

#include <memory>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/mergejoin/merge_join_build_operator.h"
#include "exec/pipeline/mergejoin/merge_join_probe_operator.h"
#include "exec/pipeline/mergejoin/merge_joiner_factory.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/vectorized/merge_joiner.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_filter_worker.h"
#include "simd/simd.h"
#include "util/runtime_profile.h"


namespace starrocks::vectorized {

MergeJoinNode::MergeJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _merge_join_node(tnode.merge_join_node),
          _join_type(tnode.merge_join_node.join_op) {

}

pipeline::OpFactories MergeJoinNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    auto rhs_operators = child(1)->decompose_to_pipeline(context);//递归，返回的是opFac
    auto lhs_operators = child(0)->decompose_to_pipeline(context);
    size_t num_right_partitions;
    size_t num_left_partitions;

            num_left_partitions = num_right_partitions = context->degree_of_parallelism();

            // Both HashJoin{Build, Probe}Operator are parallelized
            // There are two ways of shuffle//这个shuffle的目的是什么？
            // 1. If previous op is ExchangeSourceOperator and its partition type is HASH_PARTITIONED or BUCKET_SHFFULE_HASH_PARTITIONED
            // then pipeline level shuffle will be performed at sender side (ExchangeSinkOperator), so发送端shuffle接收端不shuffle
            // there is no need to perform local shuffle again at receiver side
            // 2. Otherwise, add LocalExchangeOperator
            // to shuffle multi-stream into #degree_of_parallelism# streams each of that pipes into HashJoin{Build, Probe}Operator.否则我们自己要shuffle
            TPartitionType::type part_type = TPartitionType::type::HASH_PARTITIONED;
            bool rhs_need_local_shuffle = true;//本机内shuffle是什么意思？
            if (auto* exchange_op = dynamic_cast<ExchangeSourceOperatorFactory*>(rhs_operators[0].get());//看好了，是dynamic
                exchange_op != nullptr) {//这是从计划端传过来的，有啥特殊的？
                auto& texchange_node = exchange_op->texchange_node();//这个应该就是那个source和sink中间的exchange
                DCHECK(texchange_node.__isset.partition_type);
                if (texchange_node.partition_type == TPartitionType::HASH_PARTITIONED ||
                    texchange_node.partition_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {//就是发送端是分过片，你这里的意思另外的表已经按同样规则分过片了？
                    part_type = texchange_node.partition_type;
                    rhs_need_local_shuffle = false;//所以这里指的是接收端。
                }
            }
            bool lhs_need_local_shuffle = true;
            if (auto* exchange_op = dynamic_cast<ExchangeSourceOperatorFactory*>(lhs_operators[0].get());
                exchange_op != nullptr) {
                auto& texchange_node = exchange_op->texchange_node();
                DCHECK(texchange_node.__isset.partition_type);
                if (texchange_node.partition_type == TPartitionType::HASH_PARTITIONED ||
                    texchange_node.partition_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
                    part_type = texchange_node.partition_type;
                    lhs_need_local_shuffle = false;
                }
            }

            // Make sure that local shuffle use the same hash function as the remote exchange sink do远程？
            if (rhs_need_local_shuffle) {
                if (part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
                    DCHECK(!_build_equivalence_partition_expr_ctxs.empty());
                    rhs_operators = context->maybe_interpolate_local_shuffle_exchange(
                            runtime_state(), rhs_operators, _build_equivalence_partition_expr_ctxs, part_type);
                } else {
                    rhs_operators = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), rhs_operators,
                                                                                      _build_expr_ctxs, part_type);
                }
            }
            if (lhs_need_local_shuffle) {
                if (part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
                    DCHECK(!_probe_equivalence_partition_expr_ctxs.empty());
                    lhs_operators = context->maybe_interpolate_local_shuffle_exchange(
                            runtime_state(), lhs_operators, _probe_equivalence_partition_expr_ctxs, part_type);
                } else {
                    lhs_operators = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), lhs_operators,
                                                                                      _probe_expr_ctxs, part_type);
                }
            }
        
    auto* pool = context->fragment_context()->runtime_state()->obj_pool();
    
    MergeJoinerParam param(pool, _merge_join_node, _id);
    auto merge_joiner_factory = std::make_shared<starrocks::pipeline::MergeJoinerFactory>(param, num_left_partitions);

    auto build_op = std::make_shared<MergeJoinBuildOperatorFactory>(
            context->next_operator_id(), id(), merge_joiner_factory);

    //两边用的是同一个joiner
    auto probe_op =
            std::make_shared<MergeJoinProbeOperatorFactory>(context->next_operator_id(), id(), merge_joiner_factory);

    // add build-side pipeline to context and return probe-side pipeline.
    rhs_operators.emplace_back(std::move(build_op));//这里是构建出完整的pipeline，还是部分pipeline
    context->add_pipeline(rhs_operators);//应该是这一组op组成一个pipeline

    lhs_operators.emplace_back(std::move(probe_op));

    return lhs_operators;
}


}