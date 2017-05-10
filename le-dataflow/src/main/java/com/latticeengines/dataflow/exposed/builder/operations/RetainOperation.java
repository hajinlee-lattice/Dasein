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
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;

public class RetainOperation extends Operation {

    public RetainOperation(Input input, FieldList retainedFields) {
        this(input, retainedFields, null, null);
    }

    public RetainOperation(Input input, FieldList retainedFields, FieldList groupByFields, FieldList sortingFields) {
        Pipe prior = input.pipe;
        Map<String, FieldMetadata> fmMap = new HashMap<>();
        Pipe retain;
        if (groupByFields == null) {
            retain = new Retain(prior, DataFlowUtils.convertToFields(retainedFields.getFields()));
        } else {
            Pipe groupBy;
            if (sortingFields == null) {
                groupBy = new GroupBy(prior, DataFlowUtils.convertToFields(groupByFields.getFields()));
            } else {
                groupBy = new GroupBy(prior, DataFlowUtils.convertToFields(groupByFields.getFields()),
                        DataFlowUtils.convertToFields(sortingFields.getFields()));
            }
            retain = new Retain(groupBy, DataFlowUtils.convertToFields(retainedFields.getFields()));
        }
        for (FieldMetadata fm : input.metadata) {
            fmMap.put(fm.getFieldName(), fm);
        }
        List<FieldMetadata> orderedFms = new ArrayList<>();
        for (int i = 0; i < retainedFields.getFields().length; i++) {
            String tgtField = retainedFields.getFields()[i];
            if (fmMap.containsKey(tgtField)) {
                orderedFms.add(fmMap.get(tgtField));
            } else {
                throw new IllegalArgumentException("The input metadata does not have the requested field " + tgtField);
            }
        }
        this.pipe = new Each(retain, new NoOp(),
                DataFlowUtils.convertToFields(DataFlowUtils.getFieldNames(orderedFms)));
        this.metadata = orderedFms;
    }

}
