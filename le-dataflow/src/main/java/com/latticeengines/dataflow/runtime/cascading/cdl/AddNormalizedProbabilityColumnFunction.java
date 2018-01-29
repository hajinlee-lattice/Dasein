package com.latticeengines.dataflow.runtime.cascading.cdl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class AddNormalizedProbabilityColumnFunction extends BaseOperation implements Function {

    private static final Logger log = LoggerFactory.getLogger(AddNormalizedProbabilityColumnFunction.class);
    private String scoreFieldName;

    public AddNormalizedProbabilityColumnFunction(String scoreFieldName, String normalizedFieldName) {
        super(new Fields(normalizedFieldName));
        this.scoreFieldName = scoreFieldName;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object scoreObj = arguments.getObject(scoreFieldName);
        double score = Double.parseDouble(scoreObj.toString());
        score *= 100;
        int intScore = (int) score;
        if (intScore < 5)
            intScore = 5;
        if (intScore > 99)
            intScore = 99;
        functionCall.getOutputCollector().add(new Tuple(intScore));
        return;
    }

}
