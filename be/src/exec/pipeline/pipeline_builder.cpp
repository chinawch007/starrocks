// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/pipeline_builder.h"

#include "exec/exec_node.h"

namespace starrocks::pipeline {

OpFactories PipelineBuilderContext::maybe_interpolate_local_broadcast_exchange(RuntimeState* state,
                                                                               OpFactories& pred_operators,
                                                                               int num_receivers) {
    if (num_receivers == 1) {
        return maybe_interpolate_local_passthrough_exchange(state, pred_operators);
    }

    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(state->chunk_size() * num_receivers * num_receivers);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    auto local_exchange = std::make_shared<BroadcastExchanger>(mem_mgr, local_exchange_source.get());
    auto local_exchange_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), pseudo_plan_node_id, local_exchange);
    // Add LocalExchangeSinkOperator to predecessor pipeline.
    pred_operators.emplace_back(std::move(local_exchange_sink));
    // predecessor pipeline comes to end.
    add_pipeline(pred_operators);

    OpFactories operators_source_with_local_exchange;
    // Multiple LocalChangeSinkOperators pipe into one LocalChangeSourceOperator.
    local_exchange_source->set_degree_of_parallelism(num_receivers);
    // A new pipeline is created, LocalExchangeSourceOperator is added as the head of the pipeline.
    operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
    return operators_source_with_local_exchange;
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange(RuntimeState* state,
                                                                                 OpFactories& pred_operators) {
    return maybe_interpolate_local_passthrough_exchange(state, pred_operators, 1);
}
//就是调整参数ops的并发输出度，回忆视频ppt应该是多条pipeline
OpFactories PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange(RuntimeState* state,
                                                                                 OpFactories& pred_operators,
                                                                                 int num_receivers) {
    // predecessor pipeline has multiple drivers that will produce multiple output streams, but sort operator is
    // not parallelized now and can not accept multiple streams as input, so add a LocalExchange to gather multiple
    // streams and produce one output stream piping into the sort operator.这里单独提了sort？函数的目的是把多个流凝成一个？
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());//是source，为啥取的是第0个？
    auto* source_operator = down_cast<SourceOperatorFactory*>(pred_operators[0].get());//从这里看factory的目标应该是不想早早滴把实际需要的类建出来。
    if (source_operator->degree_of_parallelism() == num_receivers) {//
        return pred_operators;
    }

    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    int buffer_size = std::max(num_receivers, static_cast<int>(source_operator->degree_of_parallelism()));
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(state->chunk_size() * buffer_size);
    auto local_exchange_source =//这个source应该就是函数的主要目标？
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    auto local_exchange = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());//它并不是个op
    auto local_exchange_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), pseudo_plan_node_id, local_exchange);
    // Add LocalExchangeSinkOperator to predecessor pipeline.
    pred_operators.emplace_back(std::move(local_exchange_sink));//这里加了一个出口？然后发到上边的source？
    // predecessor pipeline comes to end.
    add_pipeline(pred_operators);

    OpFactories operators_source_with_local_exchange;
    // Multiple LocalChangeSinkOperators pipe into one LocalChangeSourceOperator.这里是归并的意思了
    local_exchange_source->set_degree_of_parallelism(num_receivers);
    // A new pipeline is created, LocalExchangeSourceOperator is added as the head of the pipeline.
    operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
    return operators_source_with_local_exchange;//也就是说多拆出了一条pipeline，然后给调用方返回的是这条新的pipeline。
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_shuffle_exchange(
        RuntimeState* state, OpFactories& pred_operators, const std::vector<ExprContext*>& partition_expr_ctxs,
        const TPartitionType::type part_type) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());

    // If DOP is one, we needn't partition input chunks.你看看，这是要把输入chuank分区
    size_t shuffle_partitions_num = degree_of_parallelism();//这个是ctx的成员函数。
    if (shuffle_partitions_num <= 1) {
        return pred_operators;
    }

    auto* pred_source_op = down_cast<SourceOperatorFactory*>(pred_operators[0].get());//为什么都是从0开始，不可能是后边是输出吗？

    // To make sure at least one partition source operator is ready to output chunk before sink operators are full.
    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(shuffle_partitions_num * state->chunk_size());
    auto local_shuffle_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_shuffle_source->set_runtime_state(state);
    auto local_shuffle =
            std::make_shared<PartitionExchanger>(mem_mgr, local_shuffle_source.get(), part_type, partition_expr_ctxs,//这里应该就是shuffle规则
                                                 pred_source_op->degree_of_parallelism());//最后这个是输入并发度？

    // Append local shuffle sink to the tail of the current pipeline, which comes to end.
    auto local_shuffle_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), pseudo_plan_node_id, local_shuffle);
    pred_operators.emplace_back(std::move(local_shuffle_sink));
    add_pipeline(pred_operators);

    // Create a new pipeline beginning with a local shuffle source.
    OpFactories operators_source_with_local_shuffle;
    local_shuffle_source->set_degree_of_parallelism(shuffle_partitions_num);//这里就是输入跟输出并发度不同的地方了。
    operators_source_with_local_shuffle.emplace_back(std::move(local_shuffle_source));

    return operators_source_with_local_shuffle;
}

OpFactories PipelineBuilderContext::maybe_gather_pipelines_to_one(RuntimeState* state,
                                                                  std::vector<OpFactories>& pred_operators_list) {
    // If there is only one pred pipeline, we needn't local passthrough anymore.
    if (pred_operators_list.size() == 1) {
        return pred_operators_list[0];
    }

    // Approximately, each pred driver can output state->chunk_size() rows at the same time.
    size_t max_row_count = 0;
    for (const auto& pred_ops : pred_operators_list) {
        auto* source_operator = down_cast<SourceOperatorFactory*>(pred_ops[0].get());
        max_row_count += source_operator->degree_of_parallelism() * state->chunk_size();
    }

    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(max_row_count);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    auto exchanger = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());

    // Append a local exchange sink to the tail of each pipeline, which comes to end.
    for (auto& pred_operators : pred_operators_list) {
        auto local_exchange_sink =
                std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), pseudo_plan_node_id, exchanger);
        pred_operators.emplace_back(std::move(local_exchange_sink));
        add_pipeline(pred_operators);
    }

    // Create a new pipeline beginning with a local exchange source.
    OpFactories operators_source_with_local_exchange;
    local_exchange_source->set_degree_of_parallelism(degree_of_parallelism());
    operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));

    return operators_source_with_local_exchange;
}

//这里建立了node和pipeline的关联，但node是执行期node？
Pipelines PipelineBuilder::build(const FragmentContext& fragment, ExecNode* exec_node) {
    pipeline::OpFactories operators = exec_node->decompose_to_pipeline(&_context);
    _context.add_pipeline(operators);
    _context.get_pipelines().back()->set_root();
    return _context.get_pipelines();
}

} // namespace starrocks::pipeline
