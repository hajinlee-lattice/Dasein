package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.pipe.Each;
import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.strategy.DepivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.SimpleDepivotStragegyImpl;
import com.latticeengines.dataflow.runtime.cascading.DepivotFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public class DepivotOperation extends Operation {

    private final Input prior;
    private DepivotStrategy depivotStrategy;

    public DepivotOperation(Input prior, String[] targetFields, String[][] sourceFieldTuples) {
        this.prior = prior;
        List<FieldMetadata> priorMetadata = this.prior.metadata;

        Map<String, FieldMetadata> metadataMap = new HashMap<>();
        for (FieldMetadata metadata: priorMetadata) {
            metadataMap.put(metadata.getFieldName(), metadata);
        }

        List<List<FieldMetadata>> sourceFieldMetadata = new ArrayList<>();
        for (String[] sourceFieldList: sourceFieldTuples) {
            List<FieldMetadata> metadataList =  new ArrayList<>();
            for (String fieldName: sourceFieldList) {
                metadataList.add(metadataMap.get(fieldName));
            }
            sourceFieldMetadata.add(metadataList);
        }
        List<FieldMetadata> targetMetadata = new ArrayList<>();
        for (int i = 0; i < targetFields.length; i++) {
            FieldMetadata sourceField = sourceFieldMetadata.get(0).get(i);
            FieldMetadata targetField = new FieldMetadata(targetFields[i], sourceField.getJavaType());
            targetMetadata.add(targetField);
        }

        List<String> argumentFields = new ArrayList<>();
        List<List<String>> sourceFieldNameTuples = new ArrayList<>();
        for (String[] sourceFieldList: sourceFieldTuples) {
            argumentFields.addAll(Arrays.asList(sourceFieldList));
            sourceFieldNameTuples.add(Arrays.asList(sourceFieldList));
        }

        List<String> outputFields = new ArrayList<>(Arrays.asList(targetFields));
        List<FieldMetadata> outputMetadata = new ArrayList<>();
        for (FieldMetadata priorField: priorMetadata) {
            String fieldName = priorField.getFieldName();
            if (!argumentFields.contains(fieldName)) {
                outputFields.add(fieldName);
                outputMetadata.add(priorField);
            }
        }
        outputMetadata.addAll(targetMetadata);

        depivotStrategy = new SimpleDepivotStragegyImpl(sourceFieldNameTuples, Arrays.asList(targetFields));
        DepivotFunction function = new DepivotFunction(depivotStrategy, new Fields(targetFields));

        this.pipe = new Each(this.prior.pipe, function, Fields.ALL);
        this.metadata = outputMetadata;
    }
}
