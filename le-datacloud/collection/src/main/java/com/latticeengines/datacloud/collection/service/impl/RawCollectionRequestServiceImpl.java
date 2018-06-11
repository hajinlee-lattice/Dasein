package com.latticeengines.datacloud.collection.service.impl;

import java.sql.Timestamp;
import java.util.BitSet;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RawCollectionRequestMgr;
import com.latticeengines.datacloud.collection.service.RawCollectionRequestService;
import com.latticeengines.datacloud.collection.service.VendorConfigService;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

@Component
public class RawCollectionRequestServiceImpl implements RawCollectionRequestService {
    private static final Logger log = LoggerFactory.getLogger(RawCollectionRequestServiceImpl.class);

    @Inject
    private RawCollectionRequestMgr rawCollectionRequestMgr;
    @Inject
    private VendorConfigService vendorConfigService;

    public boolean addNewDomains(List<String> domains, String vendor, String reqId) {
        String vendorUpper = vendor.toUpperCase();
        if (!vendorConfigService.getVendors().contains(vendorUpper)) {
            log.warn("invalid vendor name " + vendor + ", ignore it and return...");
            return false;
        }

        vendor = vendorUpper;

        Timestamp ts = new Timestamp(System.currentTimeMillis());
        for (int i = 0; i < domains.size(); ++i) {
            RawCollectionRequest req = new RawCollectionRequest();

            req.setVendor(vendor);
            req.setTransferred(false);
            req.setRequestedTime(ts);
            req.setOriginalRequestId(reqId);
            req.setDomain(domains.get(i));

            rawCollectionRequestMgr.create(req);
        }

        return true;
    }

    public List<RawCollectionRequest> getNonTransferred() {
        return rawCollectionRequestMgr.getNonTransferred();
    }

    public void updateTransferredStatus(List<RawCollectionRequest> added, BitSet filter, boolean deleteFiltered) {
        //update transferred status
        for (int i = 0; i < added.size(); ++i) {
            if (filter.get(i))
                continue;

            added.get(i).setTransferred(true);

            rawCollectionRequestMgr.update(added.get(i));
        }

        //delete filtered items?
        if (!deleteFiltered)
            return;
        for (int i = 0; i < added.size(); ++i) {
            if (!filter.get(i))
                continue;

            rawCollectionRequestMgr.delete(added.get(i));
        }
    }
}
