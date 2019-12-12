package com.latticeengines.dataflow.runtime.cascading.propdata.source;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class BomboraDomainFunction extends BaseOperation implements Function {
    private static final long serialVersionUID = 2106968197971303837L;

    private String[] copyFrom;

    public BomboraDomainFunction(String[] copyTo, String[] copyFrom) {
        super(new Fields(copyTo));
        this.copyFrom = copyFrom;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object[] output = new Object[copyFrom.length];
        for (int i = 0; i < copyFrom.length; i++) {
            output[i] = arguments.getObject(copyFrom[i]);

        }
        functionCall.getOutputCollector().add(new Tuple(output));
    }
}
