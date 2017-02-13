package com.latticeengines.dataflow.runtime.cascading;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("rawtypes")
public class AddRowIdFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -266552390572921248L;

    private final String table;

    @ConstructorProperties({ "fieldDeclaration" })
    public AddRowIdFunction(Fields fieldDeclaration, String table) {
        super(0, fieldDeclaration);
        this.table = table;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        int currentSlice = flowProcess.getCurrentSliceNum();
        flowProcess.increment("LATTICE", table, 1L);
        long value = 10_000_000_000L * currentSlice + flowProcess.getCounterValue("LATTICE", table);
        functionCall.getOutputCollector().add(new Tuple(value));
    }

}
