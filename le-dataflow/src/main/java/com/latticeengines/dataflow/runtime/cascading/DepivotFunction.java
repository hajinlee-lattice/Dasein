package com.latticeengines.dataflow.runtime.cascading;

import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.latticeengines.dataflow.exposed.builder.strategy.DepivotStrategy;

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
    public void operate(FlowProcess flowProcess, FunctionCall functionCall )
    {
        TupleEntry arguments = functionCall.getArguments();
        List<List<Object>> valueTuples = depivotStrategy.depivot(arguments);
        if (valueTuples !=  null) {
            for (List<Object> valueTuple: valueTuples) {
                Tuple tuple = new Tuple();
                for (Object value: valueTuple) {
                    tuple.add(value);
                }
                functionCall.getOutputCollector().add( tuple );
            }
        }
    }

}
