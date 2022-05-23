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
          _merge_join_node(tnode.merge_join_node) {
    if (tnode.merge_join_node.__isset.distribution_mode) {
        _distribution_mode = tnode.merge_join_node.distribution_mode;
    }
}

Status MergeJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    //下边的这两个赋值都跟hj那边保持一致了
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.merge_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjunct.left, &ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjunct.right, &ctx));//这里确实明确标示了右侧
        _build_expr_ctxs.push_back(ctx);//可以认为是等值表达式右端.
/*
        if (eq_join_conjunct.__isset.opcode && eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) {
            _is_null_safes.emplace_back(true);
        } else {
            _is_null_safes.emplace_back(false);
        }
*/
    }

    if (tnode.merge_join_node.__isset.partition_exprs) {
        for (const auto& partition_expr : tnode.merge_join_node.partition_exprs) {//分区表达式是上边传下来的，它只传特定的部分吗？
            for (auto i = 0; i < eq_join_conjuncts.size(); ++i) {
                const auto& eq_join_conjunct = eq_join_conjuncts[i];
                if (eq_join_conjunct.left == partition_expr || eq_join_conjunct.right == partition_expr) {//不对啊，你这个分区条件不是指某个表的分区条件吗？
                    _probe_equivalence_partition_expr_ctxs.push_back(_probe_expr_ctxs[i]);
                    _build_equivalence_partition_expr_ctxs.push_back(_build_expr_ctxs[i]);
                }
            }
        }
    }

    if (tnode.merge_join_node.__isset.output_columns) {//看下这个输出是怎么搞的
        _output_slots.insert(tnode.merge_join_node.output_columns.begin(), tnode.merge_join_node.output_columns.end());
    }
    return Status::OK();
}

Status MergeJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    RETURN_IF_ERROR(Expr::prepare(_build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_probe_expr_ctxs, state));
    //RETURN_IF_ERROR(Expr::prepare(_other_join_conjunct_ctxs, state));

    return Status::OK();
}

Status MergeJoinNode::Merge(ChunkPtr* chunk) {//不用值传递而用指针，说实话我很奇怪。。。你这里还是看起项目里其他函数的用法吧。
    //获取两个chunk关联列中行的引用。
    //有没有能获取列类型的方式，这样我就能从两边chunk遍历列然后判断类型
    //可以用表达式直接从chunk上提取。
    //ColumnPtr column = _expr_ctxs->evaluate((*chunk).get());
    ASSIGN_OR_RETURN(ColumnPtr left_column, _probe_expr_ctxs[0]->evaluate((_left_chunk).get()));
    ASSIGN_OR_RETURN(ColumnPtr right_column, _build_expr_ctxs[0]->evaluate((_right_chunk).get()));
    //ColumnPtr left_column = _probe_expr_ctxs[0]->evaluate((_left_chunk).get());//把sptr传给*
    //ColumnPtr right_column = _build_expr_ctxs[0]->evaluate((_right_chunk).get());
    int left_pos = 0, right_pos = 0;
    int left_size = left_column->size(), right_size = right_column->size();

    //
    //using ColumnType = typename RunTimeTypeTraits<PT>::ColumnType;//这里是传参之后
    //ColumnHelper::as_raw_column<ColumnType>(left_chunk.key_columns[0])->get_data();

    Buffer<uint32_t> index_left, index_right;
    while(1) {
        auto res = left_column->compare_at(left_pos, right_pos, *right_column, -1);
        if (res < 0) {
            if(left_pos++ >= left_size)break;
        } else if (res > 0) {
            if(right_pos++ >= right_size)break;
        } else {//这里建立两个列的索引吧，你这里的索引大小注意下，因为可能是整个chunk的
            index_left.push_back(left_pos);
            index_right.push_back(right_pos);
        }
    }
    //对左右chunk分别添加各个需要的列。
    //能否使用这个函数也是个问题，问题在于chunk是否需要初始化其中的各个列？
    //确实是不能用。。。
    //chunk->append_selective(left_chunk, index_left, 0, index_left.size());

    for (auto merge_table_slot : right_slots) {  
        SlotDescriptor* slot = merge_table_slot.slot;//传进来的tupledesc哪去了？
        auto& column = _right_chunk->get_column_by_slot_id(slot->id());//这里的关联你需要确认下。
        if (merge_table_slot.need_output) {//从tplan一层层传下来的，就是说具体输出列，是由fe端控制的。
            ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), false);//看一下这个2参
            dest_column->append_selective(*column, index_right.data(), 0, index_right.size());//首参数引用？指针？
            (*chunk)->append_column(std::move(dest_column), slot->id());
        }
    }

    for (auto merge_table_slot : left_slots) {  
        SlotDescriptor* slot = merge_table_slot.slot;//传进来的tupledesc哪去了？
        auto& column = _left_chunk->get_column_by_slot_id(slot->id());//这里的关联你需要确认下。
        if (merge_table_slot.need_output) {//从tplan一层层传下来的，就是说具体输出列，是由fe端控制的。
            ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), false);//看一下这个2参
            dest_column->append_selective(*column, index_left.data(), 0, index_left.size());//首参数引用？指针？
            (*chunk)->append_column(std::move(dest_column), slot->id());
        }
    }

    return Status::OK();
}

Status MergeJoinNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_build_expr_ctxs, state));//什么意涵？
    RETURN_IF_ERROR(Expr::open(_probe_expr_ctxs, state));
    //RETURN_IF_ERROR(Expr::open(_other_join_conjunct_ctxs, state));

    RETURN_IF_ERROR(child(1)->open(state));
    while (true) {//这个无限循环的目的是什么？
        ChunkPtr chunk = nullptr;
        bool eos = false;
        {
            RETURN_IF_CANCELLED(state);
            // fetch chunk of right table
            RETURN_IF_ERROR(child(1)->get_next(state, &chunk, &eos));//这里的目的是为了把所有的数据囤积起来,
            if (eos) {
                break;
            }

            if (chunk->num_rows() <= 0) {//这种情况难道并不表示没拿到数据吗？
                continue;
            }
        }

        //这里是要有一个right_chunk来积累上边拿到的chunk
        _right_chunk->append(*chunk);
    }

    RETURN_IF_ERROR(child(0)->open(state));
    while (true) {//这个无限循环的目的是什么？
        ChunkPtr chunk = nullptr;
        bool eos = false;
        {
            RETURN_IF_CANCELLED(state);
            // fetch chunk of left table
            RETURN_IF_ERROR(child(0)->get_next(state, &chunk, &eos));//这里的目的是为了把所有的数据囤积起来,
            if (eos) {
                break;
            }

            if (chunk->num_rows() <= 0) {//这种情况难道并不表示没拿到数据吗？
                continue;
            }
        }

        //这里是要有一个left_chunk来积累上边拿到的chunk
        _left_chunk->append(*chunk);
    }
    //这里是要做一个对齐操作的。
    Merge(&_result_chunk);

    return Status::OK();
}

Status MergeJoinNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_CANCELLED(state);

    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    //我这里要返回对齐结果后的一定量的row，这个量该怎么确定。刚开始最简单情况，我可以都拉出来，没啥问题。
    *chunk = std::make_shared<Chunk>();
    (*chunk).swap(_result_chunk);
    _eos = true;//这里自己操作了一下，为了上边的条件，先这么搞，看看具体需不需要吧。
    *eos = false;
    return Status::OK();
}


Status MergeJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    Expr::close(_build_expr_ctxs, state);
    Expr::close(_probe_expr_ctxs, state);
    //Expr::close(_other_join_conjunct_ctxs, state);

    return ExecNode::close(state);
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
                    texchange_node.partition_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {//就是发送端是分过片，你这里的意思另外的表已经按同样规则分过片了？
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
                    texchange_node.partition_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
                    part_type = texchange_node.partition_type;
                    lhs_need_local_shuffle = false;
                }
            }

            // Make sure that local shuffle use the same hash function as the remote exchange sink do远程？
            if (rhs_need_local_shuffle) {
                if (part_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
                    //DCHECK(!_build_equivalence_partition_expr_ctxs.empty());
                    rhs_operators = context->maybe_interpolate_local_shuffle_exchange(
                            runtime_state(), rhs_operators, _build_equivalence_partition_expr_ctxs, part_type);
                } else {
                    rhs_operators = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), rhs_operators,
                                                                                      _build_expr_ctxs, part_type);
                }
            }
            if (lhs_need_local_shuffle) {
                if (part_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
                    //DCHECK(!_probe_equivalence_partition_expr_ctxs.empty());
                    lhs_operators = context->maybe_interpolate_local_shuffle_exchange(
                            runtime_state(), lhs_operators, _probe_equivalence_partition_expr_ctxs, part_type);
                } else {
                    lhs_operators = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), lhs_operators,
                                                                                      _probe_expr_ctxs, part_type);
                }
            }
        
    auto* pool = context->fragment_context()->runtime_state()->obj_pool();
    
    MergeJoinerParam param(pool, _merge_join_node, _id, //看起来这里的初始化是没什么问题的
        _build_expr_ctxs, _probe_expr_ctxs, 
        child(1)->row_desc(), child(0)->row_desc(), _output_slots);

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