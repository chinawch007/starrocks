// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <vector>

#include "exec/vectorized/hash_joiner.h"

namespace starrocks {
namespace pipeline {

using HashJoiner = starrocks::vectorized::HashJoiner;
using HashJoinerPtr = std::shared_ptr<HashJoiner>;
using HashJoiners = std::vector<HashJoinerPtr>;
class HashJoinerFactory;
using HashJoinerFactoryPtr = std::shared_ptr<HashJoinerFactory>;

class HashJoinerFactory {
public:
    HashJoinerFactory(starrocks::vectorized::HashJoinerParam& param, int dop) : _param(param), _hash_joiners(dop) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    HashJoinerPtr create_prober(int driver_sequence) {//如果probeop调俩函数传的seq都一样，那么不就是使用了同一joiner吗
        if (!_hash_joiners[driver_sequence]) {//这说明没有builder用同一seq构建。
            _param._is_buildable = is_buildable(driver_sequence);
            _hash_joiners[driver_sequence] = std::make_shared<HashJoiner>(_param, _read_only_probers);
        }

        //看到这大概会以为需要把readonly全都构建好之后再调其他地方，但其实传过去的是引用，它们构造之后我再修改也行。
        if (!_hash_joiners[driver_sequence]->is_buildable()) {//buildable就是为了这个read only
            _read_only_probers.emplace_back(_hash_joiners[driver_sequence]);
        }

        return _hash_joiners[driver_sequence];
    }

    HashJoinerPtr create_builder(int driver_sequence) {
        if (_param._distribution_mode == TJoinDistributionMode::BROADCAST) {
            driver_sequence = BROADCAST_BUILD_DRIVER_SEQUENCE;//这里是用作数组的索引，与入参其实没有任何关系了。
        }
        if (!_hash_joiners[driver_sequence]) {//这里的意思是没生成过就归属于build
            _param._is_buildable = true;//这里的意思是说生成的joiner是build吗？
            _hash_joiners[driver_sequence] = std::make_shared<HashJoiner>(_param, _read_only_probers);//probe中的调用顺序是先probe再build，但这里看似乎不是
        }

        return _hash_joiners[driver_sequence];
    }

    bool is_buildable(int driver_sequence) const {//你想想它的反例，就是广播外加不是0，于是就不是builder
        return _param._distribution_mode != TJoinDistributionMode::BROADCAST ||
               driver_sequence == BROADCAST_BUILD_DRIVER_SEQUENCE;
    }

    const HashJoiners& get_read_only_probers() const { return _read_only_probers; }

private:
    // Broadcast join need only create one hash table, because all the HashJoinProbeOperators
    // use the same hash table with their own different probe states.
    static constexpr int BROADCAST_BUILD_DRIVER_SEQUENCE = 0;

    starrocks::vectorized::HashJoinerParam _param;
    HashJoiners _hash_joiners;//管理结构就一个，其中有的是build，有的是probe
    HashJoiners _read_only_probers;
};

} // namespace pipeline
} // namespace starrocks