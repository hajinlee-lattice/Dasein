package com.latticeengines.dataflow.runtime.cascading.propdata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
abstract public class CleanupFunction extends BaseOperation implements Function
{

    private static final long serialVersionUID = 1529935996686552118L;
    private boolean removeNull;

    public CleanupFunction(Fields fieldDeclaration, boolean removeNull) {
        super(1, fieldDeclaration);
        this.removeNull = removeNull;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall )
    {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = cleanupArguments(arguments);
        if (!removeNull || result != null) {
            functionCall.getOutputCollector().add( result );
        }
    }

    protected abstract Tuple cleanupArguments(TupleEntry arguments);

}
