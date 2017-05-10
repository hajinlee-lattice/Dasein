package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Find partition boundaries of sorted ids.
 * CAUTION: This buffer needs to hold all ids in memory.
 */
public class SortPartitionBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 1423189753525474796L;

    private final String sortField;
    private final int partitions;
    private final String dummyGroupField;

    private List<Object> ids  = new ArrayList<>();

    // output fields (dummyGroupField, grpBdriesField)
    @SuppressWarnings("unchecked")
    public SortPartitionBuffer(String sortField, String dummyGroupField, String grpBdriesField, int partitions) {
        super(new Fields(dummyGroupField, grpBdriesField));
        this.sortField = sortField;
        this.dummyGroupField = dummyGroupField;
        this.partitions = partitions;
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        // there will be only one group, so group does not matter
        TupleEntry group = bufferCall.getGroup();

        // input is already sorted by id
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        while  (arguments.hasNext()) {
            TupleEntry entry = arguments.next();
            Object id = entry.getObject(sortField);
            ids.add(id);
        }

        Collections.sort(ids);
        List<String> boundaries = partitionIds();
        Tuple result = new Tuple(group.getObject(dummyGroupField), StringUtils.join(boundaries, "|"));
        bufferCall.getOutputCollector().add(result);
    }

    private List<String> partitionIds() {
        int numRows = ids.size();
        int partitionSize = numRows / partitions;
        int currentPartition = 0;
        List<String> boundaries = new ArrayList<>();
        for (Object id: ids) {
            currentPartition++;
            if (boundaries.size() < partitions - 1 && currentPartition >= partitionSize) {
                // not last partition, and current partition is not full
                currentPartition = 0;
                boundaries.add(String.valueOf(id));
            }
        }
        return boundaries;
    }


}
