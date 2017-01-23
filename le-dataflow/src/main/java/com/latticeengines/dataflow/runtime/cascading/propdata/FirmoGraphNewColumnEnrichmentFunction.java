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
public class FirmoGraphNewColumnEnrichmentFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 1L;
    private String leftMatchField;
    private String rightMatchField;
    private String enrichingField;
    @SuppressWarnings("unused")
    private String enrichedField;

    public FirmoGraphNewColumnEnrichmentFunction(String leftMatchField, String rightMatchField, String enrichingField,
            String enrichedField) {
        super(1, new Fields(enrichedField));
        this.leftMatchField = leftMatchField;
        this.rightMatchField = rightMatchField;
        this.enrichingField = enrichingField;
        this.enrichedField = enrichedField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String leftValue = arguments.getString(leftMatchField);
        String rightValue = arguments.getString(rightMatchField);
        Object enrichingValue = arguments.getObject(enrichingField);
        if (StringUtils.isNotBlank(leftValue) && StringUtils.isNotBlank(rightValue) && leftValue.equals(rightValue)) {
            functionCall.getOutputCollector().add(new Tuple(enrichingValue));
        } else {
            functionCall.getOutputCollector().add(Tuple.size(1));
        }
    }
}
