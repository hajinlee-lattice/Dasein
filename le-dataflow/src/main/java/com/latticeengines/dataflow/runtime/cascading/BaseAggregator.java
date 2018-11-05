package com.latticeengines.dataflow.runtime.cascading;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

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
    protected Map<String, Integer> namePositionMap;
    protected boolean withOptLog = false;
    protected int logFieldIdx = -1;

    public BaseAggregator(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    /**
     * If want to track operation log for buffer, please use this constructor by
     * passing withOptLog = true
     * 
     * @param fieldDeclaration
     * @param addOptLog:
     *            whether to add new column LE_OperationLogs in schema
     */
    public BaseAggregator(Fields fieldDeclaration, boolean withOptLog) {
        super(withOptLog && !StreamSupport.stream(fieldDeclaration.spliterator(), false)
                .anyMatch(field -> OperationLogUtils.DEFAULT_FIELD_NAME.equals((String) field))
                        ? fieldDeclaration.append(new Fields(OperationLogUtils.DEFAULT_FIELD_NAME))
                        : fieldDeclaration);
        namePositionMap = getPositionMap(this.fieldDeclaration);
        this.withOptLog = namePositionMap.get(OperationLogUtils.DEFAULT_FIELD_NAME) != null;
        if (this.withOptLog) {
            logFieldIdx = namePositionMap.get(OperationLogUtils.DEFAULT_FIELD_NAME);
        }
    }

    protected Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<T> aggregatorCall) {
        TupleEntry group = aggregatorCall.getGroup();
        T context = initializeContext(aggregatorCall.getGroup());
        context.dummyGroup = isDummyGroup(group);
        context.groupTuple = new TupleEntry(group);
        aggregatorCall.setContext(context);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<T> aggregatorCall) {
        T context = aggregatorCall.getContext();
        if (!context.dummyGroup) {
            TupleEntry arguments = aggregatorCall.getArguments();
            context = updateContext(context, arguments);
            aggregatorCall.setContext(context);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<T> aggregatorCall) {
        T context = aggregatorCall.getContext();
        if (!context.dummyGroup) {
            Tuple result = finalizeContext(context);
            if (result != null) {
                aggregatorCall.getOutputCollector().add(result);
            }
        } else {
            aggregatorCall.getOutputCollector().add(dummyTuple(context));
        }
    }

    protected void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    protected abstract boolean isDummyGroup(TupleEntry group);

    protected abstract T initializeContext(TupleEntry group);

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

    public static class Context {
        public TupleEntry groupTuple;
        boolean dummyGroup = false;
    }

}
