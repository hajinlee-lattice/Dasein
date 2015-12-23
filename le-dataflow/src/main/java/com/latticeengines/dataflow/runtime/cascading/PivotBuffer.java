package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotResult;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class PivotBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -5692917328708255965L;

    protected Map<String, Integer> namePositionMap;
    private PivotStrategy pivotStrategy;

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
            PivotResult pivotResult = pivotStrategy.pivot(arguments);
            if (pivotResult != null) {
                pivotResults.add(pivotResult);
            }
        }

        Collections.sort(pivotResults, new Comparator<PivotResult>() {
            @Override
            public int compare(PivotResult o1, PivotResult o2) {
                return Integer.compare(o1.getPriority(), o2.getPriority());
            }
        });

        for (PivotResult pivotResult: pivotResults) {
            Integer loc = namePositionMap.get(pivotResult.getColumnName().toLowerCase());
            result.set(loc, pivotResult.getValue());
        }
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
