package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

public final class PartitionUtils {

    public static <T> List<List<T>> partitionBySize(Collection<T> collection, int partitionSize) {
        if (partitionSize <= 0) {
            throw new IllegalArgumentException("partitionSize must be positive, but found " + partitionSize);
        }
        List<List<T>> partitions = Collections.emptyList();
        if (CollectionUtils.isNotEmpty(collection)) {
            partitions = partitionNonEmptyCollectionBySize(collection, partitionSize);
        }
        return partitions;
    }

    private static <T> List<List<T>> partitionNonEmptyCollectionBySize(Collection<T> collection,
            Integer partitionSize) {
        List<List<T>> partitions = new ArrayList<>();
        List<T> thisPartition = new ArrayList<>();
        for (T item: collection) {
            if (thisPartition.size() >= partitionSize) {
                partitions.add(thisPartition);
                thisPartition = new ArrayList<>();
            }
            thisPartition.add(item);
        }
        if (CollectionUtils.isNotEmpty(thisPartition)) {
            partitions.add(thisPartition);
        }
        return partitions;
    }
}
