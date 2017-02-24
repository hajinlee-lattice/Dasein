package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.BaseJoiner;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

public class HashJoinOperation extends Operation {

    private static Set<JoinType> supportedJoins = new HashSet<>(Arrays.asList(JoinType.INNER, JoinType.LEFT));

    public HashJoinOperation(List<Input> inputs, List<FieldList> joinFields, JoinType joinType) {
        if (!supportedJoins.contains(joinType)) {
            throw new UnsupportedOperationException("Only inner and left hash join is currently supported");
        }

        if (inputs.size() != joinFields.size()) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { "Group size mismatch" });
        }

        if (inputs.size() == 0) {
            throw new LedpException(LedpCode.LEDP_26004, new String[] { "No id defined" });
        }

        BaseJoiner joiner = null;
        switch (joinType) {
        case LEFT:
            joiner = new LeftJoin();
            break;
        default:
            joiner = new InnerJoin();
            break;
        }

        int groupSize = inputs.size();
        Pipe[] pipes = new Pipe[groupSize];
        Fields[] joinFieldsArray = new Fields[groupSize];
        List<FieldMetadata> declaredFields = null;
        Set<String> seenPipeNames = new HashSet<>();
        for (int i = 0; i < groupSize; i++) {

            String pipeName = inputs.get(i).pipe.getName();
            FieldList fieldList = joinFields.get(i);

            if (seenPipeNames.contains(pipeName)) {
                throw new IllegalStateException("There is already a pipe named " + pipeName
                        + " in the hash join. Each pipe must be uniquely named.");
            } else {
                seenPipeNames.add(pipeName);
            }

            List<FieldMetadata> metadataList = inputs.get(i).metadata;
            if (declaredFields == null) {
                declaredFields = metadataList;
            } else {
                declaredFields = DataFlowUtils.combineFields(pipeName, declaredFields, metadataList);
            }

            pipes[i] = inputs.get(i).pipe;
            joinFieldsArray[i] = DataFlowUtils.convertToFields(fieldList.getFields());
        }

        this.pipe = new HashJoin(pipes, joinFieldsArray, DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(declaredFields)), joiner);
        this.metadata = declaredFields;
    }

}
