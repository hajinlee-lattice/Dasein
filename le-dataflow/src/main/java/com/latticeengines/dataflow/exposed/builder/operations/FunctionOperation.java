package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Function;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class FunctionOperation extends Operation {

    public FunctionOperation(Input prior, String expression, FieldList sourceFields, FieldMetadata targetField) {
        ExpressionFunction function = new ExpressionFunction(new Fields(targetField.getFieldName()), //
                expression, //
                sourceFields.getFields(), //
                DataFlowUtils.getTypes(sourceFields.getFieldsAsList(), prior.metadata));

        init(prior, function, sourceFields, Collections.singletonList(targetField), null);
    }

    public FunctionOperation(Input prior, String expression, FieldList sourceFields, FieldMetadata targetField,
            FieldList outputFields) {
        ExpressionFunction function = new ExpressionFunction(new Fields(targetField.getFieldName()), //
                expression, //
                sourceFields.getFields(), //
                DataFlowUtils.getTypes(sourceFields.getFieldsAsList(), prior.metadata));

        init(prior, function, sourceFields, Collections.singletonList(targetField), outputFields);
    }

    public FunctionOperation(Input prior, Function<?> function, FieldList sourceFields, FieldMetadata targetField) {
        init(prior, function, sourceFields, Collections.singletonList(targetField), null);
    }

    public FunctionOperation(Input prior, Function<?> function, FieldList sourceFields, FieldMetadata targetField,
            FieldList outputFields) {
        init(prior, function, sourceFields, Collections.singletonList(targetField), outputFields);
    }

    public FunctionOperation(Input prior, Function<?> function, FieldList sourceFields,
            List<FieldMetadata> targetFields, FieldList outputFields) {
        init(prior, function, sourceFields, targetFields, outputFields);
    }

    public FunctionOperation(Input prior, Function<?> function, FieldList sourceFields,
            List<FieldMetadata> targetFields, FieldList outputFields, Fields overrideFieldStrategy) {
        init(prior, function, sourceFields, targetFields, outputFields, overrideFieldStrategy);
    }

    private void init(Input prior, Function<?> function, FieldList sourceFields, List<FieldMetadata> targetFields,
            FieldList outputFields) {
        init(prior, function, sourceFields, targetFields, outputFields, null);
    }

    public FunctionOperation(Input prior, Function<?> function, List<FieldMetadata> targetFields, FieldList outputFields) {
        init(prior, function, null, targetFields, outputFields);
    }

    private void init(Input prior, Function<?> function, FieldList sourceFields, List<FieldMetadata> targetFields,
            FieldList outputFields, Fields overrideFieldStrategy) {
        Fields fieldStrategy = Fields.ALL;

        List<FieldMetadata> fm = Lists.newArrayList(prior.metadata);

        Fields inputStrategy = null;
        if (sourceFields == null) {
            // by default apply to all fields
            sourceFields = new FieldList(DataFlowUtils.getFieldNames(fm));
            inputStrategy = Fields.ALL;
        }

        if (overrideFieldStrategy == null) {
            if (sourceFields.getFields().length == 1 && targetFields.size() == 1
                    && sourceFields.getFields()[0].equals(targetFields.get(0).getFieldName())) {
                fieldStrategy = Fields.REPLACE;
            }

            if (outputFields != null) {
                fieldStrategy = DataFlowUtils.convertToFields(outputFields.getFields());
            }
        } else {
            fieldStrategy = overrideFieldStrategy;
        }

        Pipe each;
        if (inputStrategy == null) {
            each = new Each(prior.pipe, DataFlowUtils.convertToFields(sourceFields.getFieldsAsList()), function,
                    fieldStrategy);
        } else {
            each = new Each(prior.pipe, Fields.ALL, function, fieldStrategy);
        }

        if (fieldStrategy != Fields.REPLACE) {
            for (FieldMetadata targetField : targetFields) {
                fm.add(targetField);
            }
            fm = DataFlowUtils.retainOnlyTheseFields(outputFields, fm);
        } else {
            Map<String, FieldMetadata> nameToFieldMetadataMap = DataFlowUtils.getFieldMetadataMap(fm);
            for (FieldMetadata targetField : targetFields) {
                FieldMetadata targetFm = nameToFieldMetadataMap.get(targetField.getFieldName());
                if (targetFm !=null && targetFm.getJavaType() != targetField.getJavaType()) {
                    FieldMetadata replaceFm = new FieldMetadata(targetField.getAvroType(), targetField.getJavaType(),
                            targetField.getFieldName(), null);
                    nameToFieldMetadataMap.put(targetField.getFieldName(), replaceFm);
                    for (int i = 0; i < fm.size(); i++) {
                        if (fm.get(i).getFieldName().equals(replaceFm.getFieldName())) {
                            fm.set(i, replaceFm);
                        }
                    }
                }
            }
        }

        addAncestry(fm, targetFields, sourceFields);

        this.pipe = each;
        this.metadata = fm;
    }

    private void addAncestry(List<FieldMetadata> fm, List<FieldMetadata> targetFields, FieldList sourceFields) {
        Map<String, FieldMetadata> map = DataFlowUtils.getFieldMetadataMap(fm);
        List<FieldMetadata> sourceFieldMetadata = new ArrayList<>();
        for (String field : sourceFields.getFields()) {
            FieldMetadata metadata = map.get(field);
            if (metadata != null) {
                sourceFieldMetadata.add(metadata);
            }
        }

        for (FieldMetadata target : targetFields) {
            target.addAncestors(sourceFieldMetadata);
        }
    }
}
