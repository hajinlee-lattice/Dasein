package com.latticeengines.ldc_collectiondb.entitymgr;

import java.sql.Timestamp;
import java.util.BitSet;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

public interface RawCollectionRequestMgr extends BaseEntityMgrRepository<RawCollectionRequest, Long> {

    List<RawCollectionRequest> getNonTransferred(int limit);

    void updateTransferred(Iterable<RawCollectionRequest> added, BitSet filter, boolean deleteFiltered);

    void saveRequests(Iterable<String> domains, String vendor, String reqId);

    void saveRequests(Iterable<String> domains, String reqId);

    void cleanupRequestsBetween(Timestamp start, Timestamp end);

    void cleanupTransferred(int batch);
}
