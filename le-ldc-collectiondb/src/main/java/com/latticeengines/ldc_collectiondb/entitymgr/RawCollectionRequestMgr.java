package com.latticeengines.ldc_collectiondb.entitymgr;

import java.sql.Timestamp;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

public interface RawCollectionRequestMgr extends BaseEntityMgrRepository<RawCollectionRequest, Long> {

    List<RawCollectionRequest> getNonTransferred(int limit);

    void saveRequests(Iterable<RawCollectionRequest> reqs);

    void saveDomains(Iterable<String> domains, String reqId);

    void cleanupRequestsBetween(Timestamp start, Timestamp end);

    void cleanupTransferred();
}
