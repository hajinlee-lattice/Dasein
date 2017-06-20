package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.latticeengines.common.exposed.collection.FileBackedOrderedList;

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
    private static final long ID_BUFFER_SIZE = 10000_000L;

    private final String sortField;
    private final int partitions;
    private final String dummyJoinKeyField;
    private final Class<?> sortFieldClz;

    private FileBackedOrderedList<?> ids;

    // output fields (dummyJoinKey, grpBdriesField)
    @SuppressWarnings("unchecked")
    public SortPartitionBuffer(String sortField, String dummyJoinKeyField, String grpBdriesField, Class<?> sortFieldClz, int partitions) {
        super(new Fields(dummyJoinKeyField, grpBdriesField));
        this.sortField = sortField;
        this.dummyJoinKeyField = dummyJoinKeyField;
        this.partitions = partitions;
        this.sortFieldClz = sortFieldClz;
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
            if (ids == null) {
                bootstrapIdList(id);
            }
            if (id instanceof Utf8) {
                id = id.toString();
            }
            ids.add(id);
        }
        List<String> boundaries = partitionIds();
        for (Long key: dummyJoinKeys) {
            Tuple result = new Tuple(key, StringUtils.join(boundaries, "|"));
            bufferCall.getOutputCollector().add(result);
        }
    }

    private void bootstrapIdList(Object id) {
        LogManager.getLogger(FileBackedOrderedList.class).setLevel(Level.DEBUG);
        if (Integer.class.equals(sortFieldClz)) {
            ids = FileBackedOrderedList.newIntList(ID_BUFFER_SIZE);
        } else if (Long.class.equals(sortFieldClz)) {
            ids = FileBackedOrderedList.newLongList(ID_BUFFER_SIZE);
        } else if (String.class.equals(sortFieldClz)) {
            ids = FileBackedOrderedList.newStrList(ID_BUFFER_SIZE);
        } else if (Float.class.equals(sortFieldClz)) {
            ids = FileBackedOrderedList.newFloatList(ID_BUFFER_SIZE);
        } else if (Double.class.equals(sortFieldClz)) {
            ids = FileBackedOrderedList.newDoubleList(ID_BUFFER_SIZE);
        }
    }

    private List<String> partitionIds() {
        int numRows = ids.size();
        int partitionSize = numRows / partitions;
        int currentPartition = 0;
        List<String> boundaries = new ArrayList<>();
        for (Object id: ids) {
            currentPartition++;
            if (id != null && boundaries.size() < partitions - 1 && currentPartition >= partitionSize) {
                // not last partition, and current partition is not full
                currentPartition = 0;
                boundaries.add(String.valueOf(id));
            }
        }
        return boundaries;
    }


}
