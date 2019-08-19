package com.latticeengines.ldc_collectiondb.entitymgr.impl;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entitymgr.CollectionWorkerMgr;
import com.latticeengines.ldc_collectiondb.repository.writer.CollectionWorkerRepository;

@Component
public class CollectionWorkerMgrImpl extends JpaEntityMgrRepositoryImpl<CollectionWorker, Long> implements CollectionWorkerMgr {

    private static final HashSet<String> WORKER_STATUS_SET = new HashSet<String>();

    static {
        WORKER_STATUS_SET.addAll(Arrays.asList(
                CollectionWorker.STATUS_NEW,
                CollectionWorker.STATUS_RUNNING,
                CollectionWorker.STATUS_FINISHED,
                CollectionWorker.STATUS_CONSUMED,
                CollectionWorker.STATUS_FAILED,
                CollectionWorker.STATUS_INGESTED));
    }

    @Inject
    private CollectionWorkerRepository collectionWorkerRepository;

    @Override
    public BaseJpaRepository<CollectionWorker, Long> getRepository() {

        return collectionWorkerRepository;

    }

    public List<CollectionWorker> getWorkerByStatus(List<String> statusList) {
        //check
        for (String status: statusList) {

            if (!WORKER_STATUS_SET.contains(status)) {

                return null;

            }

        }

        List<CollectionWorker> resultList = collectionWorkerRepository.findByStatusIn(statusList);

        return resultList;

    }

    public List<CollectionWorker> getWorkerStopped(String vendor, Timestamp after) {

        List<String> statusList = Arrays.asList(
                CollectionWorker.STATUS_CONSUMED,
                CollectionWorker.STATUS_FAILED);

        List<CollectionWorker> resultList = collectionWorkerRepository.findByStatusInAndVendorAndSpawnTimeIsAfter
                (statusList, vendor, after);

        return resultList;

    }

    public int getActiveWorkerCount(String vendor) {

        List<String> statusList = Arrays.asList(
                CollectionWorker.STATUS_NEW,
                CollectionWorker.STATUS_RUNNING);

        List<CollectionWorker> resultList = collectionWorkerRepository.findByStatusInAndVendor(statusList, vendor);
        return resultList.size();
    }

    @Override
    public void cleanupWorkerBetween(Timestamp start, Timestamp end) {
        collectionWorkerRepository.removeBySpawnTimeBetween(start, end);
    }

    @Override
    public List<CollectionWorker> getWorkerBySpawnTimeBetween(Timestamp start, Timestamp end) {
        return collectionWorkerRepository.findBySpawnTimeBetween(start, end);
    }

    @Override
    public List<CollectionWorker> getWorkerTerminatedByStatus(Timestamp after, String status) {

        if (!WORKER_STATUS_SET.contains(status)) {

            return Collections.emptyList();

        }

        return collectionWorkerRepository.findByTerminationTimeIsAfterAndStatus(after, status);
    }
}
