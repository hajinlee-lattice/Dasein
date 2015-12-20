package com.latticeengines.propdata.collection.dataflow.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

abstract public class CleanupFunction extends BaseOperation implements Function
{

    private boolean removeNull;

    public CleanupFunction(Fields fieldDeclaration, boolean removeNull) {
        super(0, fieldDeclaration);
        this.removeNull = removeNull;
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall )
    {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = cleanupArguments(arguments);
        if (!removeNull || result != null) {
            functionCall.getOutputCollector().add( result );
        }
    }

    abstract Tuple cleanupArguments(TupleEntry arguments);

}
