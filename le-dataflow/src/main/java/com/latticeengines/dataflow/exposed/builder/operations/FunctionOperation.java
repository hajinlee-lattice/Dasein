package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import cascading.operation.Function;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.google.common.collect.Lists;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;

public class FunctionOperation extends Operation {

    public FunctionOperation(Input prior, String expression, FieldList fieldsToApply, FieldMetadata targetField) {
        ExpressionFunction function = new ExpressionFunction(new Fields(targetField.getFieldName()), //
                expression, //
                fieldsToApply.getFields(), //
                DataFlowUtils.getTypes(fieldsToApply.getFieldsAsList(), prior.metadata));

        init(prior, function, fieldsToApply, Collections.singletonList(targetField), null);
    }


    public FunctionOperation(Input prior, String expression, FieldList fieldsToApply, FieldMetadata targetField,
                             FieldList outputFields) {
        ExpressionFunction function = new ExpressionFunction(new Fields(targetField.getFieldName()), //
                expression, //
                fieldsToApply.getFields(), //
                DataFlowUtils.getTypes(fieldsToApply.getFieldsAsList(), prior.metadata));

        init(prior, function, fieldsToApply, Collections.singletonList(targetField), outputFields);
    }

    public FunctionOperation(Input prior, Function<?> function, FieldList fieldsToApply, FieldMetadata targetField) {
        init(prior, function, fieldsToApply, Collections.singletonList(targetField), null);
    }

    public FunctionOperation(Input prior, Function<?> function, FieldList fieldsToApply, FieldMetadata targetField,
                             FieldList outputFields) {
        init(prior, function, fieldsToApply, Collections.singletonList(targetField), outputFields);
    }

    public FunctionOperation(Input prior, Function<?> function, FieldList fieldsToApply,
                             List<FieldMetadata> targetFields, FieldList outputFields) {
        init(prior, function, fieldsToApply, targetFields, outputFields);
    }

    private void init(Input prior, Function<?> function, FieldList fieldsToApply,
                      List<FieldMetadata> targetFields, FieldList outputFields) {
        Fields fieldStrategy = Fields.ALL;

        List<FieldMetadata> fm = Lists.newArrayList(prior.metadata);

        if (fieldsToApply.getFields().length == 1 && targetFields.size() == 1
                && fieldsToApply.getFields()[0].equals(targetFields.get(0).getFieldName())) {
            fieldStrategy = Fields.REPLACE;
        }

        if (outputFields != null) {
            fieldStrategy = DataFlowUtils.convertToFields(outputFields.getFields());
        }
        Pipe each = new Each(prior.pipe, DataFlowUtils.convertToFields(fieldsToApply.getFieldsAsList()), function,
                fieldStrategy);

        if (fieldStrategy != Fields.REPLACE) {
            for (FieldMetadata targetField : targetFields) {
                fm.add(targetField);
            }
            fm = DataFlowUtils.retainFields(outputFields, fm);
        } else {
            Map<String, FieldMetadata> nameToFieldMetadataMap = DataFlowUtils.getFieldMetadataMap(fm);
            for (FieldMetadata targetField : targetFields) {
                FieldMetadata targetFm = nameToFieldMetadataMap.get(targetField.getFieldName());
                if (targetFm.getJavaType() != targetField.getJavaType()) {
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

        this.pipe = each;
        this.metadata = fm;
    }

}
