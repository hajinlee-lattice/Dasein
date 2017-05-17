package com.latticeengines.dataflow.runtime.cascading;

import java.io.Serializable;
import java.util.Map;

import org.apache.avro.util.Utf8;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class MappingFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -29618409583081242L;

    private String sourceField;
    private Integer srcFieldId;
    private Map<Serializable, Serializable> valueMap;

    public MappingFunction(String sourceField, String targetField, Map<Serializable, Serializable> valueMap) {
        super(1, new Fields(targetField));
        this.sourceField = sourceField;
        this.valueMap = valueMap;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        findSrcFieldId(arguments);
        Object obj = arguments.getObject(srcFieldId);
        Serializable source = convertValue(obj);
        if (valueMap.containsKey(source)) {
            functionCall.getOutputCollector().add(new Tuple(valueMap.get(source)));
        } else {
            functionCall.getOutputCollector().add(Tuple.size(1));
        }
    }

    private Serializable convertValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Utf8) {
            return value.toString();
        } else {
            return (Serializable) value;
        }
    }

    private void findSrcFieldId(TupleEntry arguments) {
        if (srcFieldId == null) {
            for (int i = 0; i < arguments.size(); i++) {
                String fieldName = (String) arguments.getFields().get(i);
                if (sourceField.equals(fieldName)) {
                    srcFieldId = i;
                    return;
                }
            }
            throw new IllegalArgumentException("Cannot find specified source field " + sourceField);
        }
    }

}
