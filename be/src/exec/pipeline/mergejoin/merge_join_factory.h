#pragma once

#include <memory>
#include <vector>

#include "exec/vectorized/merge_joiner.h"

namespace starrocks {
namespace pipeline {

using MergeJoiner = starrocks::vectorized::MergeJoiner;
using MergeJoinerPtr = std::shared_ptr<MergeJoiner>;
using MergeJoiners = std::vector<MergeJoinerPtr>;
class MergeJoinerFactory;
using MergeJoinerFactoryPtr = std::shared_ptr<MergeJoinerFactory>;

class MergeJoinerFactory {
public:
    MergeJoinerFactory(starrocks::vectorized::MergeJoinerParam& param, int dop) : _param(param), _merge_joiners(dop) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    MergeJoinerPtr create_prober(int driver_sequence) {
        if (!_merge_joiners[driver_sequence]) {
            _merge_joiners[driver_sequence] = std::make_shared<MergeJoiner>(_param);
        }

        return _merge_joiners[driver_sequence];
    }

    MergeJoinerPtr create_builder(int driver_sequence) {
        if (!_merge_joiners[driver_sequence]) {
            _merge_joiners[driver_sequence] = std::make_shared<MergeJoiner>(_param);
        }

        return _merge_joiners[driver_sequence];
    }

private:
    starrocks::vectorized::MergeJoinerParam _param;
    MergeJoiners _hash_joiners;//管理结构就一个，其中有的是build，有的是probe
};

}

}