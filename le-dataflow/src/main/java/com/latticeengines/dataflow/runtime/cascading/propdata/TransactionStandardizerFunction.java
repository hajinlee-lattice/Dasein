package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.util.JsonUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class TransactionStandardizerFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -3540875319501345174L;

    private Map<String, Integer> namePositionMap;
    private String cField;
    private List<String> oldFields;
    private List<String> newFields;
    private List<String> customFields;

    public TransactionStandardizerFunction(Fields fieldDeclaration, String cField,
            List<String> oldFields, List<String> newFields, List<String> customFields) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.cField = cField;
        this.oldFields = oldFields;
        this.newFields = newFields;
        this.customFields = customFields;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        setupTupleForGroup(result, arguments);
        functionCall.getOutputCollector().add(result);
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    private void setupTupleForGroup(Tuple result, TupleEntry arguments) {
        for (String field : oldFields) {
            Integer loc = namePositionMap.get(field);
            if (loc != null && loc >= 0) {
                result.set(loc, arguments.getObject(field));
            }
        }

        for (String field : newFields) {
            Integer loc = namePositionMap.get(field);
            if (loc != null && loc >= 0) {
                result.set(loc, null);
            }
        }

        Object customObj;
        try {
            customObj = arguments.getObject(cField);
        } catch (Exception e) {
            customObj = null;
        }
        if (customObj == null) {
            Integer loc = namePositionMap.get(cField);
            Map<String, Object> customMap = new HashMap<String, Object>();
            for (String field : customFields) {
                try {
                    customMap.put(field, arguments.getObject(field));
                } catch (Exception e) {
                    continue;
                }
            }
            String jsonString = JsonUtils.serialize(customMap);
            result.set(loc, jsonString);
        }
    }
}
