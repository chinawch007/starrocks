// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/mergejoin/merge_joiner_factory.h"

namespace starrocks {
namespace pipeline {

Status MergeJoinerFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_param._build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._probe_expr_ctxs, state));
    return Status::OK();
}

void MergeJoinerFactory::close(RuntimeState* state) {
    Expr::close(_param._probe_expr_ctxs, state);
    Expr::close(_param._build_expr_ctxs, state);
}

} // namespace pipeline
} // namespace starrocks