package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.List;

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

    private String latticeColumn;
    private String dnbColumn;
    private List<String> dnbPrimaryKeys;

    public AccountMasterSeedFunction(String outputColumn, String latticeColumn, String dnbColumn,
            List<String> dnbPrimaryKeys) {
        super(new Fields(outputColumn));
        this.latticeColumn = latticeColumn;
        this.dnbColumn = dnbColumn;
        this.dnbPrimaryKeys = dnbPrimaryKeys;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        boolean fromDnb = false;
        for (String dnbPrimaryKey : dnbPrimaryKeys) {
            if (arguments.getObject(dnbPrimaryKey) != null) {
                fromDnb = true;
                break;
            }
        }
        if (fromDnb && dnbColumn != null) {
            functionCall.getOutputCollector().add(new Tuple(arguments.getObject(dnbColumn)));
        } else if (!fromDnb && latticeColumn != null) {
            functionCall.getOutputCollector().add(new Tuple(arguments.getObject(latticeColumn)));
        } else {
            functionCall.getOutputCollector().add(Tuple.size(1));
        }

    }
}
