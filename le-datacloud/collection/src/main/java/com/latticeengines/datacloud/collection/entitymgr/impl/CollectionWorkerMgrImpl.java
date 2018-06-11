package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.CollectionWorkerMgr;
import com.latticeengines.datacloud.collection.repository.CollectionWorkerRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;

@Component
public class CollectionWorkerMgrImpl extends JpaEntityMgrRepositoryImpl<CollectionWorker, Long> implements CollectionWorkerMgr {
    private static final HashSet<String> workerStatusSet = new HashSet<String>();

    static {
        workerStatusSet.add(CollectionWorker.STATUS_NEW);
        workerStatusSet.add(CollectionWorker.STATUS_RUNNING);
        workerStatusSet.add(CollectionWorker.STATUS_FINISHED);
        workerStatusSet.add(CollectionWorker.STATUS_CONSUMED);
        workerStatusSet.add(CollectionWorker.STATUS_FAILED);
    }

    @Inject
    CollectionWorkerRepository collectionWorkerRepository;

    @Override
    public BaseJpaRepository<CollectionWorker, Long> getRepository() {
        return collectionWorkerRepository;
    }

    public List<CollectionWorker> getWorkerByStatus(List<String> statusList) {
        //check
        for (int i = 0; i < statusList.size(); ++i) {
            String status = statusList.get(i);

            if (!workerStatusSet.contains(status))
                return null;
        }

        List<CollectionWorker> resultList = collectionWorkerRepository.findByStatusIn(statusList);

        return resultList;
    }

    public List<CollectionWorker> getWorkerStopped(String vendor, Timestamp after) {
        List<String> statusList = new ArrayList<>(2);
        statusList.add(CollectionWorker.STATUS_CONSUMED);
        statusList.add(CollectionWorker.STATUS_FAILED);
        List<CollectionWorker> resultList = collectionWorkerRepository.findByStatusInAndVendorAndSpawnTimeIsAfter
                (statusList, vendor, after);

        return resultList;
    }

    public int getActiveWorkerCount(String vendor) {
        List<String> statusList = new ArrayList<>(2);
        statusList.add(CollectionWorker.STATUS_NEW);
        statusList.add(CollectionWorker.STATUS_RUNNING);
        List<CollectionWorker> resultList = collectionWorkerRepository.findByStatusInAndVendor(statusList, vendor);
        return resultList.size();
    }
}
