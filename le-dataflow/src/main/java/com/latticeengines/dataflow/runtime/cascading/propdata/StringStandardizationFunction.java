package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.common.exposed.util.StringStandardizationUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class StringStandardizationFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 370788627632865263L;

    private String stringField;

    public StringStandardizationFunction(String stringField) {
        super(new Fields(stringField));
        this.stringField = stringField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String str = arguments.getString(stringField);
        str = StringStandardizationUtils.getStandardString(str);
        functionCall.getOutputCollector().add(new Tuple(str));
    }
}
