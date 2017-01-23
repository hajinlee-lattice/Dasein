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
public class FirmoGraphExistingColumnEnrichmentFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1L;
    private String leftMatchField;
    private String rightMatchField;
    private String enrichingField;
    private String enrichedField;

    public FirmoGraphExistingColumnEnrichmentFunction(String leftMatchField, String rightMatchField, String enrichingField,
            String enrichedField) {
        super(4, new Fields(leftMatchField, rightMatchField, enrichingField, enrichedField));
        this.leftMatchField = leftMatchField;
        this.rightMatchField = rightMatchField;
        this.enrichingField = enrichingField;
        this.enrichedField = enrichedField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String leftMatchValue = arguments.getString(leftMatchField);
        String rightMatchValue = arguments.getString(rightMatchField);
        Object enrichingValue = arguments.getObject(enrichingField);
        Object enrichedValue = arguments.getObject(enrichedField);
        Tuple tuple = new Tuple();
        if (StringUtils.isNotBlank(leftMatchValue) && StringUtils.isNoneBlank(rightMatchValue) && leftMatchValue.equals(rightMatchValue)) {
            tuple.add(leftMatchValue);
            tuple.add(rightMatchValue);
            tuple.add(enrichingValue);
            tuple.add(enrichingValue);
            functionCall.getOutputCollector().add(tuple);
        } else {
            tuple.add(leftMatchValue);
            tuple.add(rightMatchValue);
            tuple.add(enrichingValue);
            tuple.add(enrichedValue);
            functionCall.getOutputCollector().add(tuple);
        }
    }
}
