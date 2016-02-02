package com.latticeengines.dataflow.runtime.cascading;

import java.io.Serializable;
import java.util.Map;

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
    private Map<Serializable, Serializable> valueMap;

    public MappingFunction(String sourceField, String targetField, Map<Serializable, Serializable> valueMap) {
        super(1, new Fields(targetField));
        this.sourceField = sourceField;
        this.valueMap = valueMap;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String source = arguments.getString(sourceField);
        if (valueMap.containsKey(source)) {
            functionCall.getOutputCollector().add( new Tuple(valueMap.get(source)));
        } else {
            functionCall.getOutputCollector().add( Tuple.size(1) );
        }

    }

}
