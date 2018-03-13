package com.latticeengines.dataflow.runtime.cascading.propdata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Insight: % spend change is calculated by comparing average spend for a given
 * product of a given account in the specified time window with that of the
 * range in prior time window.
 * If lastAvgSpend is not available, spend change is -100%
 * If previousAvgSpend is not available, spend  change is 100%
 */
public class MetricsSpendChangeFunc extends BaseOperation implements Function {

    private String lastAvgSpendField;
    private String previousAvgSpendField;

    public MetricsSpendChangeFunc(String targetField, String lastAvgSpendField, String previousAvgSpendField) {
        super(new Fields(targetField));
        this.lastAvgSpendField = lastAvgSpendField;
        this.previousAvgSpendField = previousAvgSpendField;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        double lastAvgSpend = arguments.getDouble(lastAvgSpendField); // treat null as 0
        double previousAvgSpend = arguments.getDouble(previousAvgSpendField); // treat null as 0
        int spendChange;
        if (lastAvgSpend == 0 && previousAvgSpend == 0) {
            spendChange = 0;
        } else if (previousAvgSpend != 0) {
            spendChange = (int) Math.round(100 * (lastAvgSpend - previousAvgSpend) / previousAvgSpend);
        } else {
            spendChange = 100;
        }
        functionCall.getOutputCollector().add(new Tuple(spendChange));
    }
}
