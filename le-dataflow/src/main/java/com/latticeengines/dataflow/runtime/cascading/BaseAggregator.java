package com.latticeengines.dataflow.runtime.cascading;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public abstract class BaseAggregator<T extends BaseAggregator.Context> //
        extends BaseOperation<T> implements Aggregator<T> {
    private static final long serialVersionUID = 1L;

    public static class Context
    {
        boolean dummyGroup = false;
        public TupleEntry groupTuple;
    }

    protected Map<String, Integer> namePositionMap;

    protected Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName.toLowerCase(), pos++);
        }
        return positionMap;
    }

    public BaseAggregator(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public void start( FlowProcess flowProcess,
                       AggregatorCall<T> aggregatorCall )
    {
        T context = initializeContext();
        TupleEntry group = aggregatorCall.getGroup();
        context.dummyGroup = isDummyGroup(group);
        context.groupTuple = group;
        aggregatorCall.setContext( context );
    }

    public void aggregate(FlowProcess flowProcess,
                          AggregatorCall<T> aggregatorCall )
    {
        T context = aggregatorCall.getContext();
        if (!context.dummyGroup) {
            TupleEntry arguments = aggregatorCall.getArguments();
            context = updateContext(context, arguments);
            aggregatorCall.setContext( context );
        }
    }

    public void complete(FlowProcess flowProcess,
                         AggregatorCall<T> aggregatorCall )
    {
        T context = aggregatorCall.getContext();
        if (!context.dummyGroup) {
            Tuple result = finalizeContext(context);
            if (result != null) {
                aggregatorCall.getOutputCollector().add( result );
            }
        } else {
            aggregatorCall.getOutputCollector().add( dummyTuple(context) );
        }
    }

    protected abstract boolean isDummyGroup(TupleEntry group);

    protected abstract T initializeContext();

    protected abstract T updateContext(T context, TupleEntry arguments);

    protected abstract Tuple finalizeContext(T context);

    protected Tuple dummyTuple(T context) {
        TupleEntry group = context.groupTuple;
        Tuple result = Tuple.size(fieldDeclaration.size());
        for (int i = 0; i < group.size(); i++) {
            String field = group.getFields().get(i).toString();
            if (namePositionMap.containsKey(field)) {
                result.set(namePositionMap.get(field), group.getObject(i));
            }
        }
        return result;
    }

}
