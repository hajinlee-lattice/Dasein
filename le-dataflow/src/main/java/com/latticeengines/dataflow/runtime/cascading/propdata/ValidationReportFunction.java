package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.UUID;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("rawtypes")
public class ValidationReportFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -1829655353767648353L;

    private String ruleName;

    public ValidationReportFunction(String ruleName, String[] reportAttrs) {

        super(reportAttrs.length, new Fields(reportAttrs));
        this.ruleName = ruleName;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Object[] output = new Object[2];
        output[0] = ruleName;
        output[1] = UUID.randomUUID().toString();

        Tuple result = new Tuple(output);
        functionCall.getOutputCollector().add(result);
    }
}
