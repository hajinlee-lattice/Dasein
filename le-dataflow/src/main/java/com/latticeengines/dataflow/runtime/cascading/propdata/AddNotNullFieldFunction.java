package com.latticeengines.dataflow.runtime.cascading.propdata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AddNotNullFieldFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1L;
    private String fieldName;

    public AddNotNullFieldFunction(String fieldName, String targetFieldName) {
        super(new Fields(targetFieldName));
        this.fieldName = fieldName;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object value = arguments.getObject(fieldName);
        if (value != null) {
            functionCall.getOutputCollector().add(new Tuple(value));
        } else {
            functionCall.getOutputCollector().add(new Tuple(""));
        }
    }
}
