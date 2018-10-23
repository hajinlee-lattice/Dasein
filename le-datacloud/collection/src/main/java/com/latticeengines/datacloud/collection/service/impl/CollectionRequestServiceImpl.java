package com.latticeengines.datacloud.collection.service.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.PartitionUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.collection.service.CollectionRequestService;
import com.latticeengines.datacloud.collection.service.VendorConfigService;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.entitymgr.CollectionRequestMgr;

@Component
public class CollectionRequestServiceImpl implements CollectionRequestService {
    private static final Logger log = LoggerFactory.getLogger(CollectionRequestServiceImpl.class);
    private static final int RAW_REQ_BATCH = 16;
    private static final int DOMAIN_TRANSFER_BATCH = 500;

    @Inject
    private CollectionRequestMgr collectionRequestMgr;

    @Inject
    private VendorConfigService vendorConfigService;

    private ExecutorService sqlUploaders = null;

    class RawRequestCmp implements Comparator<RawCollectionRequest> {

        public int compare(RawCollectionRequest lhs, RawCollectionRequest rhs) {

            int ret = lhs.getVendor().compareToIgnoreCase(rhs.getVendor());

            if (ret == 0) {

                ret = lhs.getDomain().compareTo(rhs.getDomain());

                if (ret == 0)
                    ret = lhs.getRequestedTime().compareTo(rhs.getRequestedTime());

            }

            return ret;
        }

    }

    // sort by domain, ts desc
    class RawReqCmp implements Comparator<RawCollectionRequest> {

        public int compare(RawCollectionRequest lhs, RawCollectionRequest rhs) {
            int ret = lhs.getDomain().compareTo(rhs.getDomain());
            if (ret == 0) {
                ret = rhs.getRequestedTime().compareTo(lhs.getRequestedTime());
            }
            return ret;
        }

    }


    // each bit corresponds to one record in the list of raw reqs.
    // it denotes where that record should be "filtered"
    // filter means not to be transferred from raw req table to req table
    @VisibleForTesting
    BitSet prefilterNonTransferred(List<RawCollectionRequest> nonTransferred) {

        //sort, first by vendor, then by domain, next by time
        nonTransferred.sort(new RawRequestCmp());

        //dedup raw requests
        BitSet rawReqFilter = new BitSet(nonTransferred.size());
        int nextPos = 0;
        for (int i = 0; i < nonTransferred.size(); i = nextPos) {

            nextPos = i + 1;
            String curDomain = nonTransferred.get(i).getDomain();
            String curVendor = nonTransferred.get(i).getVendor();

            for (; nextPos < nonTransferred.size(); ++nextPos) {

                RawCollectionRequest nextRawReq = nonTransferred.get(nextPos);
                int vendorCmpRet = nextRawReq.getVendor().compareTo(curVendor);

                if (vendorCmpRet > 0 ||
                        (vendorCmpRet == 0 && nextRawReq.getDomain().compareTo(curDomain) > 0)) {
                    // switch vendor or switch domain compared to next item in the sorted list
                    // move position to next domain or vendor
                    break;
                }

            }

            // nextPos is the first item with different domain
            // mark all from i +1 to nextPos to "true"
            // meaning only i is "false"
            for (int k = i + 1; k < nextPos; ++k) {

                rawReqFilter.set(k, true);

            }

        }

        return rawReqFilter;

    }

    class ReqCmp implements Comparator<CollectionRequest> {

        public int compare(CollectionRequest lhs, CollectionRequest rhs) {

            int ret = lhs.getDomain().compareTo(rhs.getDomain());
            if (ret == 0) {

                ret = lhs.getRequestedTime().compareTo(rhs.getRequestedTime());

            }

            return ret;

        }

    }

    private void filterNonTransferredAgainstCurrent(List<RawCollectionRequest> toAdd, BitSet rawReqFilter) {

        //select * from CollectionRequest where VENDOR = vendor and DOMAIN = domain
        //select * from CollectionRequest where VENDOR in [vendors] and DOMAIN in [domains]
        //select max(REQUESTED_TIME) from CollectionRequest group by VENDOR, DOMAIN having VENDOR in [vendors] and DOMAIN in [domains]
        int next = 0;
        RawCollectionRequest[] reqBuf = new RawCollectionRequest[RAW_REQ_BATCH];
        int[] posBuf = new int[RAW_REQ_BATCH];

        for (int i = 0; i < toAdd.size(); i = next) {

            //slide through pre-filtered reqs
            for (; i < toAdd.size() && rawReqFilter.get(i); ++i) ;
            if (i == toAdd.size()) {

                break;

            }

            //accumulate micro-batch to process
            int bufLen = 0;
            int j = i;
            for (; j < toAdd.size() && bufLen < RAW_REQ_BATCH; ++j) {

                if (!rawReqFilter.get(j)) {

                    posBuf[bufLen] = j;
                    reqBuf[bufLen] = toAdd.get(j);
                    ++bufLen;

                }

            }

            //adjust next/bufLen, so that there's only one vendor to handle
            String curVendor = reqBuf[0].getVendor();
            j = bufLen - 1;
            while (reqBuf[j].getVendor().compareTo(curVendor) != 0) {

                --j;

            }
            next = posBuf[j] + 1;
            bufLen = j + 1;

            //query
            //select * from CollectionRequest where VENDOR = vendor and DOMAIN in [domains];
            List<String> domains = new ArrayList<>();
            for (j = 0; j < bufLen; ++j) {

                domains.add(reqBuf[j].getDomain());

            }
            List<CollectionRequest> resultList = collectionRequestMgr.getByVendorAndDomains(curVendor, domains);

            //sort collection req list
            resultList.sort(new ReqCmp());

            //dedup of same domain
            List<CollectionRequest> reqList = new ArrayList<>();
            for (int k = 0; k < resultList.size(); ) {

                String curDomain = resultList.get(k).getDomain();
                int l = k + 1;
                for (; l < resultList.size(); ++l) {

                    if (resultList.get(l).getDomain().compareTo(curDomain) != 0) {

                        break;

                    }

                }

                //only retain the one with largest timestamp
                reqList.add(resultList.get(l - 1));

                k = l;

            }
            resultList.clear();

            //filtering
            int reqPos = 0;
            for (int k = 0; k < bufLen && reqPos < reqList.size(); ++k) {

                //cur raw req
                RawCollectionRequest curRawReq = reqBuf[k];
                String curDomain = curRawReq.getDomain();

                //slide through collection req list, to find a possible match
                int l = reqPos;
                for (; l < reqList.size(); ++l) {

                    if (curDomain.compareTo(reqList.get(l).getDomain()) <= 0) {

                        break;

                    }

                }
                reqPos = l;

                //determine whether to filter the raw req
                if (l < reqList.size()) {

                    CollectionRequest curReq = reqList.get(l);
                    String curStatus = curReq.getStatus();
                    if (curDomain.equals(curReq.getDomain()) &&
                            (curStatus.equals(CollectionRequest.STATUS_COLLECTING) ||
                                    curStatus.equals(CollectionRequest.STATUS_READY) ||
                                    ((curStatus.equals(CollectionRequest.STATUS_DELIVERED) ||
                                            curStatus.equals(CollectionRequest.STATUS_FAILED)) &&
                                            curRawReq.getRequestedTime().getTime() - curReq.getRequestedTime().getTime() <
                                                    1000L * vendorConfigService.getCollectingFreq(curVendor)))) {

                        rawReqFilter.set(posBuf[k], true);

                    }

                }

            }

        }

    }

    public Set<String> transferRawRequests(List<RawCollectionRequest> toTransfer) {
        Set<String> allSkippedDomains = new HashSet<>();
        if (CollectionUtils.isNotEmpty(toTransfer)) {
            toTransfer.sort(new RawReqCmp());

            // dedup domains
            Set<String> domains = new HashSet<>();
            List<RawCollectionRequest> dedupList = new ArrayList<>();
            toTransfer.forEach(rawReq -> {
                if (!domains.contains(rawReq.getDomain())) {
                    domains.add(rawReq.getDomain());
                    dedupList.add(rawReq);
                }
            });

            List<Callable<Set<String>>> callables = new ArrayList<>();
            AtomicLong count = new AtomicLong(0);
            for (String vendor : vendorConfigService.getEnabledVendors()) {
                Callable<Set<String>> callable = () -> transferRawRequestsForVendor(vendor, dedupList, count);
                callables.add(callable);
            }
            // if skipped by any vendor, it is considered skipped
            // we might need to revisit this domain later on
            List<Set<String>> skippedDomainsPerVendor = ThreadPoolUtils //
                    .runCallablesInParallel(getSqlUploaders(), callables, 60, 5);
            skippedDomainsPerVendor.forEach(allSkippedDomains::addAll);
            log.info("CREATE_COLLECTION_REQ=" + count.get());
        }
        return allSkippedDomains;
    }

    // return skipped domains, they may need to be transferred later
    private Set<String> transferRawRequestsForVendor(String vendor,
            List<RawCollectionRequest> toTransfer, AtomicLong count) {
        // split into batches to reduce sql load and memory footprint for collection req table
        List<List<RawCollectionRequest>> batches = PartitionUtils //
                .partitionCollectionBySize(toTransfer, DOMAIN_TRANSFER_BATCH);
        Set<String> skippedDomains = new HashSet<>();
        batches.forEach(batch -> skippedDomains
                .addAll(transferOneBatch(vendor, batch, count)));
        return skippedDomains;
    }

    private Set<String> transferOneBatch(String vendor, List<RawCollectionRequest> toTransfer, AtomicLong count) {
        // initialize raw req map
        Map<String, RawCollectionRequest> rawReqs = new HashMap<>();
        toTransfer.forEach(rawReq -> rawReqs.put(rawReq.getDomain(), rawReq));

        // initialize last req map
        Map<String, CollectionRequest> lastReqs = new HashMap<>();
        List<CollectionRequest> sqlResult = collectionRequestMgr.getLastByVendorAndDomains(vendor,
                rawReqs.keySet());
        if (CollectionUtils.isNotEmpty(sqlResult)) {
            sqlResult.forEach(col -> lastReqs.put(col.getDomain(), col));
        }

        // check raw req one by one
        Set<String> skippedDomains = new HashSet<>();
        List<RawCollectionRequest> transferred = new ArrayList<>();
        long vendorFreq = 1000L * vendorConfigService.getCollectingFreq(vendor);
        rawReqs.forEach((domain, rawReq) -> {
            // for each raw req, check if need to be transferred
            boolean shouldTransfer;
            if (lastReqs.containsKey(domain)) {
                CollectionRequest lastReq = lastReqs.get(domain);
                shouldTransfer = shouldTransfer(rawReq, lastReq, vendorFreq);
            } else {
                shouldTransfer = true;
            }

            if (shouldTransfer) {
                transferred.add(rawReq);
            } else {
                skippedDomains.add(domain);
            }
        });

        // transfer
        transfer(vendor, transferred);
        count.addAndGet(CollectionUtils.size(transferred));

        // return skipped
        return skippedDomains;
    }

    private boolean shouldTransfer(RawCollectionRequest rawReq, CollectionRequest lastReq,
            long vendorFreq) {
        String curStatus = lastReq.getStatus();
        boolean shouldTransfer;
        switch (curStatus) {
            case CollectionRequest.STATUS_COLLECTING:
            case CollectionRequest.STATUS_READY:
                shouldTransfer = false;
                break;
            case CollectionRequest.STATUS_DELIVERED:
            case CollectionRequest.STATUS_FAILED:
                long lastTs = lastReq.getRequestedTime().getTime();
                long rawTs = rawReq.getRequestedTime().getTime();
                shouldTransfer = (rawTs - lastTs) > vendorFreq;
                break;
            default:
                shouldTransfer = true;
                break;
        }
        return shouldTransfer;
    }

    private void transfer(String vendor, List<RawCollectionRequest> rawReqs) {
        List<CollectionRequest> reqs = new ArrayList<>();
        rawReqs.forEach(rawReq -> {
            CollectionRequest colReq = new CollectionRequest();
            colReq.setVendor(vendor);
            colReq.setDomain(rawReq.getDomain());
            colReq.setOriginalRequestId(rawReq.getOriginalRequestId());
            colReq.setRequestedTime(rawReq.getRequestedTime());
            colReq.setRetryAttempts(0);
            colReq.setStatus(CollectionRequest.STATUS_READY);
            colReq.setPickupTime(null);
            colReq.setPickupWorker(null);
            colReq.setDeliveryTime(null);
            reqs.add(colReq);
        });
        collectionRequestMgr.saveRequests(reqs);
        log.info("Transferred " + CollectionUtils.size(rawReqs) //
                + " raw requests to requests table for vendor " + vendor);
    }

    @Override
    public BitSet addNonTransferred(List<RawCollectionRequest> toAdd) {

        //sort and pre-filter raw req
        BitSet rawReqFilter = prefilterNonTransferred(toAdd);
        if (rawReqFilter.cardinality() == toAdd.size()) {

            return rawReqFilter;

        }

        //continue to filter raw req against current req
        filterNonTransferredAgainstCurrent(toAdd, rawReqFilter);

        //insert collection req
        for (int i = 0; i < toAdd.size(); ++i) {

            if (rawReqFilter.get(i)) {

                continue;

            }

            RawCollectionRequest rawReq = toAdd.get(i);

            CollectionRequest colReq = new CollectionRequest();
            colReq.setVendor(rawReq.getVendor());
            colReq.setDomain(rawReq.getDomain());
            colReq.setOriginalRequestId(rawReq.getOriginalRequestId());
            colReq.setRequestedTime(rawReq.getRequestedTime());
            colReq.setRetryAttempts(0);
            colReq.setStatus(CollectionRequest.STATUS_READY);
            colReq.setPickupTime(null);
            colReq.setPickupWorker(null);
            colReq.setDeliveryTime(null);

            collectionRequestMgr.create(colReq);

        }

        return rawReqFilter;
    }

    @Override
    public void beginCollecting(List<CollectionRequest> readyReqs, CollectionWorker worker) {

        Timestamp pickupTime = new Timestamp(System.currentTimeMillis());
        String workerId = worker.getWorkerId();
        for (CollectionRequest req : readyReqs) {

            req.setPickupWorker(workerId);
            req.setPickupTime(pickupTime);
            req.setStatus(CollectionRequest.STATUS_COLLECTING);

            collectionRequestMgr.update(req);

        }

    }

    @Override
    public int handlePending(String vendor, int maxRetries, List<CollectionWorker> finishedWorkers) {

        //check vendor & list
        vendor = vendor.toUpperCase();
        if (!vendorConfigService.getVendors().contains(vendor) ||
                CollectionUtils.isEmpty(finishedWorkers)) {

            return 0;

        }

        //normalize max retry count
        if (maxRetries <= 0) {

            maxRetries = vendorConfigService.getDefMaxRetries();

        }

        List<CollectionRequest> resultList = collectionRequestMgr.getPending(vendor, finishedWorkers);

        //processing
        int resetCount = 0, failCount = 0;
        for (CollectionRequest req: resultList) {

            int reqRetries = req.getRetryAttempts();

            if (reqRetries < maxRetries) {

                req.setRetryAttempts(reqRetries + 1);
                req.setPickupTime(null);
                req.setPickupWorker(null);
                req.setStatus(CollectionRequest.STATUS_READY);

                ++resetCount;

            } else {

                req.setStatus(CollectionRequest.STATUS_FAILED);

                ++failCount;

            }

            collectionRequestMgr.update(req);

        }

        if (failCount > 0) {

            log.info(failCount + " pending requests marked as failed");

        }
        resultList.clear();

        return resetCount;

    }

    @Override
    public int consumeFinished(String workerId, Set<String> domains) {

        List<CollectionRequest> reqs = collectionRequestMgr.getDelivered(workerId);

        Timestamp ts = new Timestamp(System.currentTimeMillis());
        for (CollectionRequest req: reqs) {

            if (domains.contains(req.getDomain())) {

                req.setStatus(CollectionRequest.STATUS_DELIVERED);
                req.setDeliveryTime(ts);

                collectionRequestMgr.update(req);

            }

        }

        int ret = reqs.size();
        reqs.clear();

        return ret;

    }

    @Override
    public Timestamp getEarliestTime(String vendor, String status) {

        //check vendor
        vendor = vendor.toUpperCase();
        if (!vendorConfigService.getVendors().contains(vendor)) {

            return new Timestamp(System.currentTimeMillis());

        }

        //check status
        status = status.toUpperCase();
        if (!status.equals(CollectionRequest.STATUS_READY) &&
                !status.equals(CollectionRequest.STATUS_COLLECTING) &&
                !status.equals(CollectionRequest.STATUS_DELIVERED) &&
                !status.equals(CollectionRequest.STATUS_FAILED)) {

            return new Timestamp(System.currentTimeMillis());

        }

        return collectionRequestMgr.getEarliestTime(vendor, status);

    }

    @Override
    public List<CollectionRequest> getReady(String vendor, int upperLimit) {

        vendor = vendor.toUpperCase();
        if (!vendorConfigService.getVendors().contains(vendor)) {

            return Collections.emptyList();

        }

        if (upperLimit < 0) {

            upperLimit = vendorConfigService.getDefCollectionBatch();

        }

        return collectionRequestMgr.getReady(vendor, upperLimit);

    }

    private ExecutorService getSqlUploaders() {
        if (sqlUploaders == null) {
            synchronized (this) {
                if (sqlUploaders == null) {
                    sqlUploaders = ThreadPoolUtils.getFixedSizeThreadPool("raw-req-transfer", 16);
                }
            }
        }
        return sqlUploaders;
    }

}
