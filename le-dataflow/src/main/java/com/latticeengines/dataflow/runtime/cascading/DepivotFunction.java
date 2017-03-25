package com.latticeengines.dataflow.runtime.cascading;

import com.latticeengines.dataflow.exposed.builder.strategy.DepivotStrategy;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DepivotFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 7569335493684363695L;

    private DepivotStrategy depivotStrategy;

    protected DepivotFunction(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    public DepivotFunction(DepivotStrategy depivotStrategy, Fields fieldDeclaration) {
        this(fieldDeclaration);
        this.depivotStrategy = depivotStrategy;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Iterable<Tuple> valueTuples = depivotStrategy.depivot(arguments);
        for (Tuple valueTuple : valueTuples) {
            functionCall.getOutputCollector().add( valueTuple );
        }
    }

}
