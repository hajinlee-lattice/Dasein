package com.latticeengines.dataflow.runtime.cascading.cdl;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class TrimFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1461026240384987251L;
    private String trimField;

    public TrimFunction(String trimField) {
        super(new Fields(trimField));
        this.trimField = trimField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String fieldValue = arguments.getString(trimField);
        if (StringUtils.isEmpty(fieldValue)) {
            functionCall.getOutputCollector().add(new Tuple(fieldValue));
        } else {
            fieldValue = fieldValue.trim();
            fieldValue = fieldValue.replaceAll("\\s+", " ");
            functionCall.getOutputCollector().add(new Tuple(fieldValue));
        }

    }
}
