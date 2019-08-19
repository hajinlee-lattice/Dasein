package com.latticeengines.ldc_collectiondb.entitymgr;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;

public interface CollectionRequestMgr extends BaseEntityMgrRepository<CollectionRequest, Long> {

    List<CollectionRequest> getByVendorAndDomains(String vendor, Collection<String> domains);

    List<CollectionRequest> getReady(String vendor, int upperLimit);

    List<CollectionRequest> getPending(String vendor, List<CollectionWorker> finishedWorkers);

    Timestamp getEarliestTime(String vendor, String status);

    List<CollectionRequest> getDelivered(String pickupWorker);

    void cleanupRequestBetween(Timestamp start, Timestamp end);

    void cleanupRequests(Collection<String> statuses, String vendor, Timestamp before, int batch);
}
