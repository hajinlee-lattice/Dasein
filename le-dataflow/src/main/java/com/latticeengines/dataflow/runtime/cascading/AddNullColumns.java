package com.latticeengines.dataflow.runtime.cascading;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AddNullColumns extends BaseOperation implements Function {

    private static final long serialVersionUID = -266552390572921248L;
    
    /**
     * Function that will be used for populating default values.
     * @param fieldDeclaration set of fields that will be set to null.
     */
    @ConstructorProperties({ "fieldDeclaration" })
    public AddNullColumns(Fields fieldDeclaration) {
        super(0, fieldDeclaration);
    }
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Fields fieldDeclaration = getFieldDeclaration();
        Object[] values = new Object[fieldDeclaration.size()];
        TupleEntry entry = new TupleEntry(fieldDeclaration, new Tuple(values), false);
        
        functionCall.getOutputCollector().add(entry);
    }

}
