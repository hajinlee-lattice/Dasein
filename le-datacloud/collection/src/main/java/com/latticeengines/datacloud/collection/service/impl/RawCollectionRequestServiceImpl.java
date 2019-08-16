package com.latticeengines.datacloud.collection.service.impl;

import java.sql.Timestamp;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.collection.service.RawCollectionRequestService;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.RawCollectionRequestMgr;

@Component
public class RawCollectionRequestServiceImpl implements RawCollectionRequestService {

    private static final Logger log = LoggerFactory.getLogger(RawCollectionRequestServiceImpl.class);

    @Inject
    private RawCollectionRequestMgr rawCollectionRequestMgr;

    private ExecutorService uploadWorkers;

    @Value("${datacloud.collection.req.transfer.batch}")
    private int rawReqTransferBatch;

    @Value("${datacloud.collection.cleanup.batch}")
    private int cleanupBatch;

    public boolean addNewDomains(List<String> domains, String vendor, String reqId) {

        final String vendorUpper = vendor.toUpperCase();
        if (!VendorConfig.EFFECTIVE_VENDOR_SET.contains(vendorUpper)) {

            log.warn("invalid vendor name " + vendor + ", ignore it and return...");
            return false;

        }

        /*
        try (PerformanceTimer timer = new PerformanceTimer("Saved in total " //
                + CollectionUtils.size(domains) + " raw requests to db.")) {

            int chunkSize = 1000;
            List<List<String>> domainPartitions = PartitionUtils.partitionCollectionBySize(domains, chunkSize);
            List<Runnable> uploaders = new ArrayList<>();

            domainPartitions.forEach(partition -> {

                Runnable uploader = () -> {

                    try (PerformanceTimer timer2 = new PerformanceTimer(
                            "Saved a chunk of " + partition.size() + " raw requests to db.")) {

                        Timestamp ts = new Timestamp(System.currentTimeMillis());
                        List<RawCollectionRequest> reqs = partition.stream() //
                                .map(domain -> RawCollectionRequest.generate(vendorUpper, domain, ts, reqId))//
                                .collect(Collectors.toList());

                        rawCollectionRequestMgr.saveRequests(reqs);

                    }

                };

                uploaders.add(uploader);

            });

            if (CollectionUtils.size(uploaders) == 1) {

                uploaders.get(0).run();

            } else {

                ThreadPoolUtils.runRunnablesInParallel(getUploadWorkers(), uploaders, //
                        60, 5);

            }

        }*/
        rawCollectionRequestMgr.saveRequests(domains, vendor, reqId);

        return true;

    }

    public List<RawCollectionRequest> getNonTransferred() {

        return rawCollectionRequestMgr.getNonTransferred(rawReqTransferBatch);

    }

    public void updateTransferredStatus(List<RawCollectionRequest> added, BitSet filter, boolean deleteFiltered) {

        //update transferred status
        for (int i = 0; i < added.size(); ++i) {

            if (filter.get(i)) {

                continue;

            }

            added.get(i).setTransferred(true);

            rawCollectionRequestMgr.update(added.get(i));

        }

        //delete filtered items?
        if (!deleteFiltered) {

            return;

        }
        for (int i = 0; i < added.size(); ++i) {

            if (!filter.get(i)) {

                continue;

            }

            rawCollectionRequestMgr.delete(added.get(i));

        }

    }

    private static RawCollectionRequest toRawRequest(String vendor, Timestamp timestamp, String reqId, String domain) {

        RawCollectionRequest req = new RawCollectionRequest();
        req.setVendor(vendor);
        req.setTransferred(false);
        req.setRequestedTime(timestamp);
        req.setOriginalRequestId(reqId);
        req.setDomain(domain);

        return req;

    }

    private ExecutorService getUploadWorkers() {

        if (uploadWorkers == null) {

            synchronized (this) {

                if (uploadWorkers == null) {

                    uploadWorkers = ThreadPoolUtils.getFixedSizeThreadPool("raw-req-upload", 4);

                }

            }

        }

        return uploadWorkers;

    }

    @Override
    public void cleanupRequestsBetween(Timestamp start, Timestamp end) {
        rawCollectionRequestMgr.cleanupRequestsBetween(start, end);
    }

    @Override
    public void cleanup() {

        rawCollectionRequestMgr.cleanupTransferred(cleanupBatch);

    }
}
