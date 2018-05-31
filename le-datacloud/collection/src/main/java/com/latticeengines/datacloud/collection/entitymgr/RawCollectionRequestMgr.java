package com.latticeengines.datacloud.collection.entitymgr;

import java.util.BitSet;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

public interface RawCollectionRequestMgr extends BaseEntityMgrRepository<RawCollectionRequest, Long> {
    List<RawCollectionRequest> getNonTransferred();

    //List<RawCollectionRequest> getNonTransferred(String vendor);
    void updateTransferredStatus(List<RawCollectionRequest> added, BitSet filter, boolean deleteFiltered);

    boolean addNewDomains(List<String> domains, String vendor, String reqId);
}
