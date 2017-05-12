package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import edu.emory.mathcs.backport.java.util.Collections;

/**
 * Find partition boundaries of sorted ids.
 * CAUTION: This buffer needs to hold all ids in memory.
 */
public class SortPartitionBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 1423189753525474796L;

    private final String sortField;
    private final int partitions;
    private final String dummyJoinKeyField;

    private List<Object> ids  = new ArrayList<>();

    // output fields (dummyJoinKey, grpBdriesField)
    @SuppressWarnings("unchecked")
    public SortPartitionBuffer(String sortField, String dummyJoinKeyField, String grpBdriesField, int partitions) {
        super(new Fields(dummyJoinKeyField, grpBdriesField));
        this.sortField = sortField;
        this.dummyJoinKeyField = dummyJoinKeyField;
        this.partitions = partitions;
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        // there will be only one group, so group does not matter

        // input is already sorted by id
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        Set<Long> dummyJoinKeys = new HashSet<>();
        while  (arguments.hasNext()) {
            TupleEntry entry = arguments.next();
            Object id = entry.getObject(sortField);
            dummyJoinKeys.add(entry.getLong(dummyJoinKeyField));
            ids.add(id);
        }

        Collections.sort(ids);
        List<String> boundaries = partitionIds();
        for (Long key: dummyJoinKeys) {
            Tuple result = new Tuple(key, StringUtils.join(boundaries, "|"));
            bufferCall.getOutputCollector().add(result);
        }
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
