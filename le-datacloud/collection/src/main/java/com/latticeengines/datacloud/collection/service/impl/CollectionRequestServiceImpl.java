package com.latticeengines.datacloud.collection.service.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.datacloud.collection.service.CollectionRequestService;
import com.latticeengines.datacloud.collection.service.VendorConfigService;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.CollectionRequestMgr;

@Component
public class CollectionRequestServiceImpl implements CollectionRequestService {
    private static final Logger log = LoggerFactory.getLogger(CollectionRequestServiceImpl.class);

    @Inject
    private CollectionRequestMgr collectionRequestMgr;
    @Inject
    private VendorConfigService vendorConfigService;

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

    private BitSet prefilterNonTransferred(List<RawCollectionRequest> nonTransferred) {

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

                    break;

                }

            }

            for (int k = i + 1; k < nextPos; ++k) {

                rawReqFilter.set(k, true);

            }

        }

        //filter invalid domains
        for (int i = 0; i < nonTransferred.size(); ++i) {

            if (rawReqFilter.get(i)) {

                continue;

            }

            if (StringUtils.isEmpty(DomainUtils.parseDomain(nonTransferred.get(i).getDomain()))) {

                rawReqFilter.set(i, true);

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

    private static final int RAW_REQ_BATCH = 16;
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
        if (!VendorConfig.EFFECTIVE_VENDOR_SET.contains(vendor) ||
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

            log.info(failCount + " pending " + vendor + " collection requests marked as failed");

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
        if (!VendorConfig.EFFECTIVE_VENDOR_SET.contains(vendor)) {

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
        if (!VendorConfig.EFFECTIVE_VENDOR_SET.contains(vendor)) {

            return Collections.emptyList();

        }

        if (upperLimit < 0) {

            upperLimit = vendorConfigService.getDefCollectionBatch();

        }

        return collectionRequestMgr.getReady(vendor, upperLimit);

    }

    @Override
    public void cleanupRequestsBetween(Timestamp start, Timestamp end) {
        collectionRequestMgr.cleanupRequestBetween(start, end);
    }


    @Override
    public void cleanupRequestHandled(String vendor, Timestamp before) {
        List<String> statuses = Arrays.asList(CollectionRequest.STATUS_DELIVERED, CollectionRequest.STATUS_FAILED);

        collectionRequestMgr.cleanupRequests(statuses, vendor, before);

    }
}
