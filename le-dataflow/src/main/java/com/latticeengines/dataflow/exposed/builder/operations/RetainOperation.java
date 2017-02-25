package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.NoOp;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;

public class RetainOperation extends Operation {

    public RetainOperation(Input input, FieldList retainedFields, boolean inOrder) {
        Pipe prior = input.pipe;
        Map<String, FieldMetadata> fmMap = new HashMap<>();

        boolean needRetain = false;
        for (String field: retainedFields.getFields()) {
            if (fmMap.containsKey(field)) {
                needRetain = true;
                break;
            }
        }
        Pipe retain = prior;
        if (needRetain) {
            retain = new Retain(retain, DataFlowUtils.convertToFields(retainedFields.getFields()));
        }

        if (inOrder) {
            for (FieldMetadata fm: input.metadata) {
                fmMap.put(fm.getFieldName(), fm);
            }
            boolean needSort = false;
            List<FieldMetadata> orderedFms = new ArrayList<>();
            for (int i = 0; i < retainedFields.getFields().length; i++) {
                String tgtField = retainedFields.getFields()[i];
                String srcField = input.metadata.get(i).getFieldName();
                if (!srcField.equalsIgnoreCase(tgtField)) {
                    needSort = true;
                    orderedFms.add(fmMap.get(tgtField));
                }
            }
            if (needSort) {
                this.pipe = new Each(retain, new NoOp(), DataFlowUtils.convertToFields(retainedFields.getFields()));
            } else {
                this.pipe = retain;
            }
            this.metadata = orderedFms;
        } else {
            List<FieldMetadata> fm = new ArrayList<>(input.metadata);
            this.pipe = retain;
            this.metadata = DataFlowUtils.retainOnlyTheseFields(retainedFields, fm);
        }
    }

}
