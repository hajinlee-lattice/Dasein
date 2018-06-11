package com.latticeengines.datacloud.collection.service;

import java.util.BitSet;
import java.util.List;

import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

public interface RawCollectionRequestService {
    boolean addNewDomains(List<String> domains, String vendor, String reqId);
    List<RawCollectionRequest> getNonTransferred();
    void updateTransferredStatus(List<RawCollectionRequest> added, BitSet filter, boolean deleteFiltered);
}
