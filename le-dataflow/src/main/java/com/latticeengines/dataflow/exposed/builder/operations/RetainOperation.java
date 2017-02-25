package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.NoOp;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;

public class RetainOperation extends Operation {

    private static final Log log = LogFactory.getLog(RetainOperation.class);

    public RetainOperation(Input input, FieldList retainedFields) {
        Pipe prior = input.pipe;
        Map<String, FieldMetadata> fmMap = new HashMap<>();

        boolean needRetain = false;
        for (String field: retainedFields.getFields()) {
            if (!fmMap.containsKey(field)) {
                needRetain = true;
                break;
            }
        }
        Pipe retain = prior;
        if (needRetain) {
            retain = new Retain(retain, DataFlowUtils.convertToFields(retainedFields.getFields()));
        } else {
            log.info("The fields in the node is already the same as declared fields. No need to retain.");
        }

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
