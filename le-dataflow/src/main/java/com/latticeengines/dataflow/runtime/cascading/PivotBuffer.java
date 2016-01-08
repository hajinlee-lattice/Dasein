package com.latticeengines.dataflow.runtime.cascading;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotResult;

@SuppressWarnings("rawtypes")
public class PivotBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -5692917328708255965L;

    protected Map<String, Integer> namePositionMap;
    private PivotStrategy pivotStrategy;
    private Map<Integer, Set<Comparable<Serializable>>> countContextMap = new HashMap<>();

    protected PivotBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public PivotBuffer(PivotStrategy pivotStrategy, Fields fieldDeclaration) {
        this(fieldDeclaration);
        this.pivotStrategy = pivotStrategy;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field: fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName.toLowerCase(), pos++);
        }
        return positionMap;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);

        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(result, arguments);

        bufferCall.getOutputCollector().add(result);
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field: fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName.toLowerCase());
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    private void setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        populateDefault(result);

        List<PivotResult> pivotResults = new ArrayList<>();
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            pivotResults.addAll(pivotStrategy.pivot(arguments));
        }

        for (PivotResult pivotResult: pivotResults) {
            Integer loc = namePositionMap.get(pivotResult.getColumnName().toLowerCase());
            result.set(loc, aggregateValue(result.getObject(loc), loc, pivotResult));
        }
    }

    private Object aggregateValue(Object oldValue, Integer loc, PivotResult result) {
        switch (result.getPivotType()) {
            case ANY:
                return aggregateAny(oldValue, result.getValue());
            case MAX:
                return aggregateMax(oldValue, result.getValue());
            case MIN:
                return aggregateMin(oldValue, result.getValue());
            case SUM:
                return aggregateSum(oldValue, result.getValue());
            case COUNT:
                return aggregateCount(loc, result.getValue());
            case EXISTS:
                return aggregateExists(oldValue, result.getValue());
            default:
                return result.getValue();
        }
    }

    private static Object aggregateAny(Object oldValue, Object newValue) {
        if (newValue != null) {
            return newValue;
        } else {
            return oldValue;
        }
    }

    @SuppressWarnings("unchecked")
    private static Object aggregateMax(Object oldValue, Object newValue) {
        if (oldValue == null) {
            return newValue;
        } else if (newValue == null) {
            return oldValue;
        } else {
            Comparable<Object> comparable = (Comparable<Object>) newValue;
            return comparable.compareTo(oldValue) >= 0 ? newValue : oldValue;
        }
    }

    @SuppressWarnings("unchecked")
    private static Object aggregateMin(Object oldValue, Object newValue) {
        if (oldValue == null) {
            return newValue;
        } else if (newValue == null) {
            return oldValue;
        } else {
            Comparable<Object> comparable = (Comparable<Object>) newValue;
            return comparable.compareTo(oldValue) <= 0 ? newValue : oldValue;
        }
    }

    private static Object aggregateSum(Object oldValue, Object newValue) {
        if (oldValue == null) {
            return newValue;
        } else if (newValue == null) {
            return oldValue;
        } else {
            if (oldValue instanceof Integer) {
                return (Integer) oldValue + (Integer) newValue;
            } else if (oldValue instanceof Long) {
                return (Long) oldValue + (Long) newValue;
            } else if (oldValue instanceof Double) {
                return (Double) oldValue + (Double) newValue;
            } else if (oldValue instanceof Float) {
                return (Float) oldValue + (Float) newValue;
            } else {
                return null;
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    private Object aggregateCount(Integer loc, Object newValue) {
        Comparable<Serializable> comparable = (Comparable<Serializable>) newValue;
        if (countContextMap.containsKey(loc)) {
            Set<Comparable<Serializable>> valueSet = countContextMap.get(loc);
            valueSet.add(comparable);
            countContextMap.put(loc, valueSet);
        } else {
            Set<Comparable<Serializable>> valueSet = new HashSet<>(Collections.singleton(comparable));
            countContextMap.put(loc, valueSet);
        }

        if (newValue instanceof Long) {
            return (long) countContextMap.get(loc).size();
        } else {
            return countContextMap.get(loc).size();
        }
    }

    private static Object aggregateExists(Object oldValue, Object newValue) {
        return (Boolean) oldValue || (newValue != null);
    }

    private void populateDefault(Tuple result) {
        for (Map.Entry<String, Object> entry: pivotStrategy.getDefaultValues().entrySet()) {
            String column = entry.getKey();
            Integer loc = namePositionMap.get(column.toLowerCase());
            if (loc != null && loc >= 0) {
                result.set(loc, entry.getValue());
            }
        }
    }

}
