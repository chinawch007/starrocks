// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/merge_joiner.h"

#include <runtime/runtime_state.h>

#include <memory>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_filter_worker.h"
#include "simd/simd.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

MergeJoiner::MergeJoiner(const MergeJoinerParam& param)
        : _merge_join_node(param._merge_join_node),
          _pool(param._pool),
          _build_expr_ctxs(param._build_expr_ctxs),//看下怎么传过来有效的参数
          _probe_expr_ctxs(param._probe_expr_ctxs),
          _right_row_descriptor(param._right_row_descriptor),
          _left_row_descriptor(param._left_row_descriptor),
          _output_slots(param._output_slots) {

    for (const auto& tuple_desc : _right_row_descriptor.tuple_descriptors()) {
        for (const auto& slot : tuple_desc->slots()) {
            MergeTableSlotDescriptor merge_table_slot;
            merge_table_slot.slot = slot;
            if (_output_slots.empty() ||
                std::find(_output_slots.begin(), _output_slots.end(), slot->id()) !=
                        _output_slots.end() ) {
                merge_table_slot.need_output = true;
            } else {
                merge_table_slot.need_output = false;
            }

            right_slots.emplace_back(merge_table_slot);
        }
    }

    for (const auto& tuple_desc : _left_row_descriptor.tuple_descriptors()) {
        for (const auto& slot : tuple_desc->slots()) {
            MergeTableSlotDescriptor merge_table_slot;
            merge_table_slot.slot = slot;
            if (_output_slots.empty() ||
                std::find(_output_slots.begin(), _output_slots.end(), slot->id()) !=
                        _output_slots.end() ) {
                merge_table_slot.need_output = true;
            } else {
                merge_table_slot.need_output = false;
            }

            left_slots.emplace_back(merge_table_slot);
        }
    }
}

Status MergeJoiner::prepare_builder(RuntimeState* state, RuntimeProfile* runtime_profile) {
    if (_runtime_state == nullptr) {
        _runtime_state = state;//这变量没看见用啊
    }

    return Status::OK();
}

Status MergeJoiner::prepare_prober(RuntimeState* state, RuntimeProfile* runtime_profile) {
    if (_runtime_state == nullptr) {
        _runtime_state = state;
    }

    return Status::OK();
}

//就是组装个大chunk，得换个名
Status MergeJoiner::append_chunk_to_buffer(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _right_chunk->append(*chunk);
    return Status::OK();
}

bool MergeJoiner::need_input() const {
    return _phase == MergeJoinPhase::PROBE;
}

bool MergeJoiner::has_output() const {//此处要注意会不会出现eos阶段来取块的情况
    if (_phase == MergeJoinPhase::POST_PROBE && _result_chunk != NULL) {
        return true;
    }

    return false;
}
/*
void MergeJoiner::sort_buffer(RuntimeState* state) {
    std::vector<ExprContext*> sort_exprs;
    std::vector<bool>* is_asc;
    std::vector<bool>* is_null_first;
    //二参可以对过之后再仔细看看怎么设置，毕竟要花点时间。
    ChunksSorterFullSort chunk_sorter(state, &sort_exprs, &is_asc, &is_null_first, 1000);//最后的数之后看着改改。
    chunk_sorter.update(_right_chunk);
    chunk_sorter.done();
}
*/
void MergeJoiner::Merge(ChunkPtr chunk) {//两个排好序的chunk合成一个chunk
    //获取两个chunk关联列中行的引用。
    //有没有能获取列类型的方式，这样我就能从两边chunk遍历列然后判断类型
    //可以用表达式直接从chunk上提取。
    //ColumnPtr column = _expr_ctxs->evaluate((*chunk).get());
    ColumnPtr left_column = _probe_expr_ctxs->evaluate(*_left_chunk);
    ColumnPtr right_column = _build_expr_ctxs->evaluate(*_right_chunk);
    int left_pos = 0, right_pos = 0;
    int left_size = left_column->size(), right_size = right_column->size()

    //
    //using ColumnType = typename RunTimeTypeTraits<PT>::ColumnType;//这里是传参之后
    //ColumnHelper::as_raw_column<ColumnType>(left_chunk.key_columns[0])->get_data();

    Buffer<uint32_t> index_left, index_right;
    while(1) {
        auto res = left_column->compare_at(left_pos, right_pos, right_column, -1);
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
        auto& column = (*_right_chunk)->get_column_by_slot_id(slot->id());//这里的关联你需要确认下。
        if (merge_table_slot.need_output) {//从tplan一层层传下来的，就是说具体输出列，是由fe端控制的。
            ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), false);//看一下这个2参
            dest_column->append_selective(column, index_right, 0, index_right.size());//首参数引用？指针？
            chunk->append_column(std::move(dest_column), slot->id());
        }
    }

    for (auto merge_table_slot : left_slots) {  
        SlotDescriptor* slot = merge_table_slot.slot;//传进来的tupledesc哪去了？
        auto& column = (*_right_chunk)->get_column_by_slot_id(slot->id());//这里的关联你需要确认下。
        if (merge_table_slot.need_output) {//从tplan一层层传下来的，就是说具体输出列，是由fe端控制的。
            ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), false);//看一下这个2参
            dest_column->append_selective(column, index_left, 0, index_left.size());//首参数引用？指针？
            chunk->append_column(std::move(dest_column), slot->id());
        }
    }

    //这几行是从join_hash_map.tpp中搞过来的，不知道有没有其他的往chunk中添加数据的模式。
    /*
    for (column : right_chunk.columns) {
        if (column == right_column)continue;
        //ColumnPtr dest_column = ColumnHelper::create_column(TypeDescriptor, false);
        //我这里能不能生成一个空列，跟源列一个类型的。
        ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), to_nullable);
        dest_column->append_selective(*column, right_index, 0, right_index.size());
        (*chunk)->append_column(std::move(dest_column), slot->id());//它里面的map是不是说你插入column的顺序是随意的，但由map来索引
    }
    */

    
}

void MergeJoiner::push_chunk(RuntimeState* state, ChunkPtr&& chunk) {
    DCHECK(chunk && !chunk->is_empty());

    _left_chunk->append(*chunk);
}

//我拿走了一整块，可能需要个swap，保证安全性，还要调一个set_finished
//这也是用sptr的理由
StatusOr<ChunkPtr> MergeJoiner::pull_chunk(RuntimeState* state) {
    //DCHECK(_phase != MergeJoinPhase::BUILD);
    auto chunk = std::make_shared<Chunk>();

    if (_phase == MergeJoinPhase::PROBE || _result_chunk != nullptr) {
        _result_chunk.swap(chunk);
    }

    return chunk;
}

void MergeJoiner::close(RuntimeState* state) {
    
}

}



