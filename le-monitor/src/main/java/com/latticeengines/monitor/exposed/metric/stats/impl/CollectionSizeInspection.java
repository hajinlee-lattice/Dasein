package com.latticeengines.monitor.exposed.metric.stats.impl;

import java.util.Collection;

import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.latticeengines.monitor.exposed.metric.stats.Inspection;

public class CollectionSizeInspection extends AbstractIntegerInspection implements Inspection {

    private final Collection<?> collection;

    public CollectionSizeInspection(ThreadPoolTaskScheduler scheduler, Collection<?> collection, String collectionName) {
        this.collection = collection;
        this.setScheduler(scheduler);
        this.setInspectionName(collectionName);
        this.bootstrap();
    }

    @Override
    protected Integer getCurrentValue() {
        synchronized (collection) {
            return collection.size();
        }
    }

    @Override
    public String toString() {
        return "Collection Size Inspection [" + inspectionName + "] [" + hashCode() + "]";
    }

}
