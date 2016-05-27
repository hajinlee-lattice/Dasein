package com.latticeengines.dataflow.runtime.cascading;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("rawtypes")
public class AddRowId extends BaseOperation implements Function {

    private static final long serialVersionUID = -266552390572921248L;
    
    private final String table;

    @ConstructorProperties({ "fieldDeclaration" })
    public AddRowId(Fields fieldDeclaration, String table) {
        super(0, fieldDeclaration);
        this.table = table;
    }
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        flowProcess.increment("LATTICE", table, 1L);
        long value = flowProcess.getCounterValue("LATTICE", table);
        functionCall.getOutputCollector().add(new Tuple(value));
    }

}
