package com.latticeengines.datacloud.collection.service;

import java.sql.Timestamp;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

public interface CollectionRequestService {

    BitSet addNonTransferred(List<RawCollectionRequest> toAdd);

    Set<String> transferRawRequests(List<RawCollectionRequest> toTransfer);

    void beginCollecting(List<CollectionRequest> readyReqs, CollectionWorker worker);

    int handlePending(String vendor, int maxRetries, List<CollectionWorker> finishedWorkers);

    int consumeFinished(String workerId, Set<String> domains);

    Timestamp getEarliestTime(String vendor, String status);

    List<CollectionRequest> getReady(String vendor, int upperLimit);

}
