package com.latticeengines.dataflow.runtime.cascading.propdata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ProfileSampleFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -8514685809263871665L;

    private String attr;
    private double ratio;

    public ProfileSampleFunction(String attr, double ratio) {
        super(new Fields(attr));
        this.attr = attr;
        this.ratio = ratio;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (arguments.getObject(attr) == null) {
            return;
        }
        if (Math.random() <= ratio) {
            functionCall.getOutputCollector().add(new Tuple(arguments.getObject(attr)));
        }
    }

}
