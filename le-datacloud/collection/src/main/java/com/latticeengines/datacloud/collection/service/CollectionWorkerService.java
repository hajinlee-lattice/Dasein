package com.latticeengines.datacloud.collection.service;

import java.sql.Timestamp;
import java.util.List;

import com.latticeengines.ldc_collectiondb.entitymgr.CollectionWorkerMgr;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;

public interface CollectionWorkerService {

    int getActiveWorkerCount(String vendor);

    CollectionWorkerMgr getEntityMgr();

    List<CollectionWorker> getWorkerByStatus(List<String> status);

    List<CollectionWorker> getWorkerStopped(String vendor, Timestamp after);

    void cleanupWorkerBetween(Timestamp start, Timestamp end);

    List<CollectionWorker> getWorkerBySpawnTimeBetween(Timestamp start, Timestamp end);

}
