package com.latticeengines.dataflow.runtime.cascading;

import com.latticeengines.dataflow.exposed.builder.strategy.AddFieldStrategy;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AddFieldFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 337831829609802778L;
    private AddFieldStrategy strategy;

    public AddFieldFunction(AddFieldStrategy strategy, Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.strategy = strategy;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall )
    {
        TupleEntry arguments = functionCall.getArguments();
        Object value = strategy.compute(arguments);
        if (value !=  null) {
            functionCall.getOutputCollector().add( new Tuple(value) );
        }
    }

}
