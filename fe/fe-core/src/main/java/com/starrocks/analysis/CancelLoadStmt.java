// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;

public class CancelLoadStmt extends DdlStmt {

    private String dbName;
    private String label;

    private Expr whereClause;

    public String getDbName() {
        return dbName;
    }

    public String getLabel() {
        return label;
    }

    public CancelLoadStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException("No database selected");
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }

        // check auth after we get real load job

        // analyze expr if not null
        boolean valid = true;
        do {
            if (whereClause == null) {
                valid = false;
                break;
            }

            if (whereClause instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break;
                }
            } else {
                valid = false;
                break;
            }

            // left child
            if (!(whereClause.getChild(0) instanceof SlotRef)) {
                valid = false;
                break;
            }
            if (!((SlotRef) whereClause.getChild(0)).getColumnName().equalsIgnoreCase("label")) {
                valid = false;
                break;
            }

            // right child
            if (!(whereClause.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break;
            }

            label = ((StringLiteral) whereClause.getChild(1)).getStringValue();
            if (Strings.isNullOrEmpty(label)) {
                valid = false;
                break;
            }
        } while (false);

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\"");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CANCEL LOAD ");
        if (!Strings.isNullOrEmpty(dbName)) {
            stringBuilder.append("FROM " + dbName);
        }

        if (whereClause != null) {
            stringBuilder.append(" WHERE " + whereClause.toSql());
        }
        return stringBuilder.toString();
    }

}
