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

    public RetainOperation(Input input, FieldList retainedFields) {
        Pipe prior = input.pipe;
        Map<String, FieldMetadata> fmMap = new HashMap<>();
        Pipe retain = new Retain(prior, DataFlowUtils.convertToFields(retainedFields.getFields()));
        for (FieldMetadata fm: input.metadata) {
            fmMap.put(fm.getFieldName(), fm);
        }
        List<FieldMetadata> orderedFms = new ArrayList<>();
        for (int i = 0; i < retainedFields.getFields().length; i++) {
            String tgtField = retainedFields.getFields()[i];
            String srcField = input.metadata.get(i).getFieldName();
            if (!srcField.equalsIgnoreCase(tgtField)) {
                orderedFms.add(fmMap.get(tgtField));
            }
        }
        this.pipe = new Each(retain, new NoOp(), DataFlowUtils.convertToFields(retainedFields.getFields()));
        this.metadata = orderedFms;
    }

}
