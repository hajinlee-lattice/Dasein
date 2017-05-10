package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SortPartitionFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 5495338743901277641L;

    private final String partitionField;
    private final String grpBdriesField;
    private final String sortingField;
    private final Class<Comparable<?>> sortingFieldClz;
    private List<Comparable<?>> boundaries;

    public SortPartitionFunction(String partitionField, String grpBdriesField, String sortingField,
            Class<Comparable<?>> sortingFieldClz) {
        super(new Fields(partitionField));
        this.partitionField = partitionField;
        this.grpBdriesField = grpBdriesField;
        this.sortingField = sortingField;
        this.sortingFieldClz = sortingFieldClz;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (boundaries == null) {
            boundaries = extractBoundaries(arguments.getString(grpBdriesField));
        }
        Object sortingValue = arguments.getObject(sortingField);
        int partition = findPartition(sortingValue);
        functionCall.getOutputCollector().add(new Tuple(partition));
    }

    private List<Comparable<?>> extractBoundaries(String str) {
        List<Comparable<?>> boundaries = new ArrayList<>();
        for (String b : str.split("\\|")) {
            if (sortingFieldClz.equals(Long.class)) {
                boundaries.add(Long.valueOf(b));
            } else if (sortingFieldClz.equals(Integer.class)) {
                boundaries.add(Integer.valueOf(b));
            } else if (sortingFieldClz.equals(String.class)) {
                boundaries.add(b);
            } else {
                throw new IllegalArgumentException(
                        "Only support Integer, Long and String for the sorting field. But found "
                                + sortingFieldClz.getSimpleName());
            }
        }
        return boundaries;
    }

    private int findPartition(Object sortingValue) {
        if (sortingValue == null) {
            return 0;
        }
        int i;
        for (i = 0; i < boundaries.size(); i++) {
            if (sortingFieldClz.equals(Long.class)) {
                if (((Long) boundaries.get(i)).compareTo((Long) sortingValue) >= 0) {
                    break;
                }
            } else if (sortingFieldClz.equals(Integer.class)) {
                if (((Integer) boundaries.get(i)).compareTo((Integer) sortingValue) >= 0) {
                    break;
                }
            } else if (sortingFieldClz.equals(String.class)) {
                if (((String) boundaries.get(i)).compareTo(sortingValue.toString()) >= 0) {
                    break;
                }
            } else {
                throw new IllegalArgumentException(
                        "Only support Integer, Long and String for the sorting field. But found "
                                + sortingValue.getClass().getSimpleName());
            }
        }
        return i;
    }
}
