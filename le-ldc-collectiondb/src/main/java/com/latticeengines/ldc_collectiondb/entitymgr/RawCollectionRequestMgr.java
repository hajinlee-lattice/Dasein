package com.latticeengines.ldc_collectiondb.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

public interface RawCollectionRequestMgr extends BaseEntityMgrRepository<RawCollectionRequest, Long> {

    List<RawCollectionRequest> getNonTransferred();

    void saveRequests(Iterable<RawCollectionRequest> reqs);

}
