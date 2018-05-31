package com.latticeengines.datacloud.collection.entitymgr;

import java.sql.Timestamp;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

public interface CollectionRequestMgr extends BaseEntityMgrRepository<CollectionRequest, Long> {
    BitSet addNonTransferred(List<RawCollectionRequest> toAdd);

    List<CollectionRequest> getReady(String vendor, int upperLimit);

    void beginCollecting(List<CollectionRequest> readyReqs, CollectionWorker worker);

    int getPending(String vendor, int maxRetries, List<CollectionWorker> finishedWorkers);

    Timestamp getEarliestRequestedTime(String vendor, String status);

    int consumeRequests(String workerId, Set<String> domains);
}
