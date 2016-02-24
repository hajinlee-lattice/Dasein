package com.latticeengines.dataflow.runtime.cascading;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class StringTruncateFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -5156204500560462184L;
    private String fieldName;
    private Integer targetLength;

    public StringTruncateFunction(String fieldName, Integer targetLength) {
        super(new Fields(fieldName));
        this.fieldName = fieldName;
        this.targetLength = targetLength;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall )
    {
        TupleEntry arguments = functionCall.getArguments();
        String value = arguments.getString(fieldName);
        if (StringUtils.isNotEmpty(value) && value.length() > targetLength) {
            functionCall.getOutputCollector().add( new Tuple(value.substring(0, targetLength)) );
        } else {
            functionCall.getOutputCollector().add( new Tuple(value) );
        }
    }

}
