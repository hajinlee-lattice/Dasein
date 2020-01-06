package com.latticeengines.datacloud.collection.service;

import java.sql.Timestamp;
import java.util.List;

import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entitymgr.CollectionWorkerMgr;

public interface CollectionWorkerService {

    List<CollectionWorker> getActiveWorker(String vendor);

    CollectionWorkerMgr getEntityMgr();

    List<CollectionWorker> getWorkerByStatus(List<String> status);

    void cleanupWorkerBetween(Timestamp start, Timestamp end);

    List<CollectionWorker> getWorkerBySpawnTimeBetween(Timestamp start, Timestamp end);

}
