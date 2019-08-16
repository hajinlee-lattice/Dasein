package com.latticeengines.ldc_collectiondb.entitymgr;

import java.sql.Timestamp;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;

public interface CollectionWorkerMgr extends BaseEntityMgrRepository<CollectionWorker, Long> {

    //void updateStatus(String taskArn, String status);
    //int cleanOutdated(Timestamp before);
    //int updateNewWorkerStatus(List<String> activeTaskArns);

    List<CollectionWorker> getWorkerByStatus(List<String> status);

    List<CollectionWorker> getWorkerStopped(String vendor, Timestamp after);

    int getActiveWorkerCount(String vendor);

    void cleanupWorkerBetween(Timestamp start, Timestamp end);

    List<CollectionWorker> getWorkerBySpawnTimeBetween(Timestamp start, Timestamp end);

    List<CollectionWorker> getWorkerTerminatedByStatus(Timestamp after, String status);
}
