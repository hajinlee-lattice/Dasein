package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.pipe.CoGroup;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.BaseJoiner;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.pipe.joiner.RightJoin;

public class JoinOperation extends Operation {

    public JoinOperation(Input lhs, FieldList lhsJoinFields, Input rhs, FieldList rhsJoinFields, JoinType joinType,
            boolean hashJoin) {

        List<FieldMetadata> declaredFields = DataFlowUtils.combineFields(rhs.pipe.getName(), lhs.metadata,
                rhs.metadata);

        BaseJoiner joiner = null;

        switch (joinType) {
        case LEFT:
            joiner = new LeftJoin();
            break;
        case RIGHT:
            joiner = new RightJoin();
            break;
        case OUTER:
            joiner = new OuterJoin();
            break;
        default:
            joiner = new InnerJoin();
            break;
        }

        Pipe join = null;
        if (!hashJoin) {
            join = new CoGroup(lhs.pipe, //
                    DataFlowUtils.convertToFields(lhsJoinFields.getFields()), //
                    rhs.pipe, //
                    DataFlowUtils.convertToFields(rhsJoinFields.getFields()), //
                    DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(declaredFields)), //
                    joiner);
        } else {
            join = new HashJoin(lhs.pipe, //
                    DataFlowUtils.convertToFields(lhsJoinFields.getFields()), //
                    rhs.pipe, //
                    DataFlowUtils.convertToFields(rhsJoinFields.getFields()), //
                    DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(declaredFields)), //
                    joiner);
        }

        this.pipe = join;
        this.metadata = new ArrayList<>(declaredFields);
    }

}
