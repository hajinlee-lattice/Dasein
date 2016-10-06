package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AccountMasterSeedFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 704139437526099546L;

    private boolean takeAllFromDnB;
    private String dnbColumn;
    private String leColumn;
    private Map<String, String> setDnBColumnValues;

    public AccountMasterSeedFunction(String outputColumn, String dnbColumn, String leColumn, boolean takeAllFromDnB,
            Map<String, String> setDnBColumnValues) {
        super(new Fields(outputColumn));
        this.leColumn = leColumn;
        this.dnbColumn = dnbColumn;
        this.takeAllFromDnB = takeAllFromDnB;
        this.setDnBColumnValues = setDnBColumnValues;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (setDnBColumnValues.containsKey(dnbColumn)) {
            functionCall.getOutputCollector().add(new Tuple(setDnBColumnValues.get(dnbColumn)));
        } else if (takeAllFromDnB) {
            functionCall.getOutputCollector().add(new Tuple(arguments.getObject(dnbColumn)));
        } else if (!takeAllFromDnB && leColumn != null) {
            functionCall.getOutputCollector().add(new Tuple(arguments.getObject(leColumn)));
        } else {
            functionCall.getOutputCollector().add(Tuple.size(1));
        }
    }
}
