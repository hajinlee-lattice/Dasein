package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class FieldEnrichmentFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1L;
    private String fromField;
    private String toField;

    public FieldEnrichmentFunction(String fromField, String toField) {
        super(2, new Fields(fromField, toField));
        this.fromField = fromField;
        this.toField = toField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object fromValue = arguments.getObject(fromField);
        Object toValue = arguments.getObject(toField);
        Tuple tuple = new Tuple();
        if (fromValue == null) {
            tuple.add(fromValue);
            tuple.add(toValue);
            functionCall.getOutputCollector().add(tuple);
            return;
        }
        if (toValue == null) {
            tuple.add(fromValue);
            tuple.add(fromValue);
            functionCall.getOutputCollector().add(tuple);
            return;
        }
        String fromValueStr = fromValue.toString();
        String toValueStr = toValue.toString();
        if (!StringUtils.isBlank(fromValueStr) && StringUtils.isBlank(toValueStr)) {
            tuple.add(fromValue);
            tuple.add(fromValue);
            functionCall.getOutputCollector().add(tuple);
        } else {
            tuple.add(fromValue);
            tuple.add(toValue);
            functionCall.getOutputCollector().add(tuple);
        }

    }
}
