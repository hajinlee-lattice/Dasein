package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.CollectionRequestMgr;
import com.latticeengines.datacloud.collection.repository.CollectionRequestRepository;
import com.latticeengines.datacloud.collection.repository.reader.CollectionRequestReaderRepository;
import com.latticeengines.datacloud.collection.util.CollectionDBUtil;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

@Component
public class CollectionRequestMgrImpl extends JpaEntityMgrRepositoryImpl<CollectionRequest, Long> implements CollectionRequestMgr {
    static final int RAW_REQ_BATCH = 16;
    @Inject
    CollectionRequestRepository collectionRequestRepository;
    @Inject
    CollectionRequestReaderRepository collectionRequestReaderRepository;

    @Override
    public BaseJpaRepository<CollectionRequest, Long> getRepository() {
        return collectionRequestRepository;
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
                if (vendorCmpRet > 0 || (vendorCmpRet == 0 && nextRawReq.getDomain().compareTo(curDomain) > 0))
                    break;
            }

            for (int k = i + 1; k < nextPos; ++k)
                rawReqFilter.set(k, true);
        }

        return rawReqFilter;
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
            if (i == toAdd.size())
                break;

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
            while (reqBuf[j].getVendor().compareTo(curVendor) != 0)
                --j;
            next = posBuf[j] + 1;
            bufLen = j + 1;

            //query
            //select * from CollectionRequest where VENDOR = vendor and DOMAIN in [domains];
            Class<CollectionRequest> reqType = CollectionRequest.class;
            EntityManager entityManager = collectionRequestRepository.getEntityManager();

            CriteriaBuilder builder = entityManager.getCriteriaBuilder();
            CriteriaQuery<CollectionRequest> query = builder.createQuery(reqType);
            Root<CollectionRequest> reqTable = query.from(reqType);
            List<String> domains = new ArrayList<>();
            for (j = 0; j < bufLen; ++j)
                domains.add(reqBuf[j].getDomain());
            query.select(reqTable).where(builder.and(builder.equal(reqTable.get("vendor"), curVendor), reqTable.get("domain").in(domains)));
            TypedQuery<CollectionRequest> typedQuery = entityManager.createQuery(query);
            List<CollectionRequest> resultList = typedQuery.getResultList();

            //sort collection req list
            resultList.sort(new ReqCmp());

            //dedup of same domain
            List<CollectionRequest> reqList = new ArrayList<>();
            for (int k = 0; k < resultList.size(); ) {
                String curDomain = resultList.get(k).getDomain();
                int l = k + 1;
                for (; l < resultList.size(); ++l) {
                    if (resultList.get(l).getDomain().compareTo(curDomain) != 0)
                        break;
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
                    if (curDomain.compareTo(reqList.get(l).getDomain()) <= 0)
                        break;
                }
                reqPos = l;

                //determine whether to filter the raw req
                if (l < reqList.size()) {
                    CollectionRequest curReq = reqList.get(l);
                    if (curDomain.compareTo(curReq.getDomain()) == 0 && (
                            curReq.getStatus().compareTo(CollectionRequest.STATUS_COLLECTING) == 0 || curReq.getStatus().compareTo(CollectionRequest.STATUS_READY) == 0 || (
                                    curReq.getStatus().compareTo(CollectionRequest.STATUS_DELIVERED) == 0 && curRawReq.getRequestedTime().getTime() - curReq.getRequestedTime().getTime() < 1000L * CollectionDBUtil.getVendorCollectingFreq(curVendor))))
                        rawReqFilter.set(posBuf[k], true);
                }
            }
        }
    }

    public BitSet addNonTransferred(List<RawCollectionRequest> toAdd) {
        //sort and pre-filter raw req
        BitSet rawReqFilter = prefilterNonTransferred(toAdd);
        if (rawReqFilter.cardinality() == toAdd.size())
            return rawReqFilter;
        //System.out.println("pre filter raw reqs: " + rawReqFilter.cardinality() + " filtered");

        //continue to filter raw req against current req
        filterNonTransferredAgainstCurrent(toAdd, rawReqFilter);

        //insert collection req
        for (int i = 0; i < toAdd.size(); ++i) {
            if (rawReqFilter.get(i))
                continue;

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

            collectionRequestRepository.save(colReq);
        }

        return rawReqFilter;
    }

    public List<CollectionRequest> getReady(String vendor, int upperLimit) {
        //check upper limit
        if (upperLimit <= 0)
            upperLimit = CollectionDBUtil.DEF_COLLECTION_BATCH;

        //check vendor
        vendor = vendor.toUpperCase();
        if (!CollectionDBUtil.getVendors().contains(vendor))
            return null;

        //query
        //select * from CollectionRequest where VENDOR = vendor and STATUS = 'READY' order by REQUESTED_TIME asc;
        Class<CollectionRequest> reqType = CollectionRequest.class;
        EntityManager entityManager = collectionRequestReaderRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CollectionRequest> query = builder.createQuery(reqType);
        Root<CollectionRequest> reqTable = query.from(reqType);
        query.select(reqTable).where(builder.and(builder.equal(reqTable.get("vendor"), vendor), builder.equal(reqTable.get("status"), CollectionRequest.STATUS_READY))).orderBy(builder.asc(reqTable.get("requestedTime")));
        TypedQuery<CollectionRequest> typedQuery = entityManager.createQuery(query);
        List<CollectionRequest> resultList = typedQuery.setMaxResults(upperLimit).getResultList();

        return resultList;
    }

    public void beginCollecting(List<CollectionRequest> readyReqs, CollectionWorker worker) {
        Timestamp pickupTime = new Timestamp(System.currentTimeMillis());
        String workerId = worker.getWorkerId();
        for (CollectionRequest req : readyReqs) {
            req.setPickupWorker(workerId);
            req.setPickupTime(pickupTime);
            req.setStatus(CollectionRequest.STATUS_COLLECTING);
        }

        collectionRequestRepository.saveAll(readyReqs);
    }

    public int getPending(String vendor, int maxRetries, List<CollectionWorker> finishedWorkers) {
        //check vendor
        vendor = vendor.toUpperCase();
        if (!CollectionDBUtil.getVendors().contains(vendor))
            return 0;

        //check worker list
        if (finishedWorkers.size() == 0)
            return 0;

        List<String> finishedWorkerIds = new ArrayList<>(finishedWorkers.size());
        for (int i = 0; i < finishedWorkers.size(); ++i)
            finishedWorkerIds.add(finishedWorkers.get(i).getWorkerId());

        //normalize max retry count
        if (maxRetries <= 0)
            maxRetries = CollectionDBUtil.DEF_MAX_RETRIES;

        //query
        //select * from CollectionRequest where VENDOR = vendor and STATUS = 'COLLECTING' and PICKUP_WORKER in [finished worker ids];
        Class<CollectionRequest> reqType = CollectionRequest.class;
        EntityManager entityManager = collectionRequestReaderRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CollectionRequest> query = builder.createQuery(reqType);
        Root<CollectionRequest> reqTable = query.from(reqType);
        query.select(reqTable)
                .where(builder.and(
                        builder.equal(reqTable.get("vendor"), vendor),
                        builder.notEqual(reqTable.get("status"), CollectionRequest.STATUS_DELIVERED),
                        builder.notEqual(reqTable.get("status"), CollectionRequest.STATUS_FAILED),
                        reqTable.get("pickupWorker").in(finishedWorkerIds)));
        //FIXME: does failed requests needs to be activated again (within retry limit)?
        TypedQuery<CollectionRequest> typedQuery = entityManager.createQuery(query);
        List<CollectionRequest> resultList = typedQuery.getResultList();

        //processing
        for (int i = 0; i < resultList.size(); ++i) {
            CollectionRequest curReq = resultList.get(i);
            int reqRetries = curReq.getRetryAttempts() + 1;
            curReq.setRetryAttempts(reqRetries);
            if (reqRetries < maxRetries) {
                curReq.setPickupTime(null);
                curReq.setPickupWorker(null);
                curReq.setStatus(CollectionRequest.STATUS_READY);
            } else {
                curReq.setStatus(CollectionRequest.STATUS_FAILED);
            }

            collectionRequestRepository.save(curReq);
        }

        int ret = resultList.size();
        resultList.clear();

        return ret;
    }

    public int consumeRequests(String workerId, Set<String> domains) {

        Class<CollectionRequest> reqType = CollectionRequest.class;
        EntityManager entityManager = collectionRequestReaderRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CollectionRequest> update = builder.createQuery(reqType);
        Root<CollectionRequest> reqTable = update.from(reqType);
        /*
        update.set(reqTable.get("status"), CollectionRequest.STATUS_DELIVERED)
                .set(reqTable.get("deliveryTime"), new Timestamp(System.currentTimeMillis()))
                .where(builder.equal(reqTable.get("pickupWorker"), workerId));*/
        update.select(reqTable).where(builder.equal(reqTable.get("pickupWorker"), workerId));
        List<CollectionRequest> resultList = entityManager.createQuery(update).getResultList();

        Timestamp ts = new Timestamp(System.currentTimeMillis());
        for (int i = 0; i < resultList.size(); ++i) {
            CollectionRequest req = resultList.get(i);
            if (domains.contains(req.getDomain())) {
                req.setStatus(CollectionRequest.STATUS_DELIVERED);
                req.setDeliveryTime(ts);

                collectionRequestRepository.save(req);
            }
        }

        int ret = resultList.size();
        resultList.clear();
        return ret;
    }

    public Timestamp getEarliestRequestedTime(String vendor, String status) {
        //check
        vendor = vendor.toUpperCase();
        if (!CollectionDBUtil.getVendors().contains(vendor))
            return new Timestamp(System.currentTimeMillis());

        status = status.toUpperCase();
        if (!status.equals(CollectionRequest.STATUS_READY) &&
                !status.equals(CollectionRequest.STATUS_COLLECTING) &&
                !status.equals(CollectionRequest.STATUS_DELIVERED) &&
                !status.equals(CollectionRequest.STATUS_FAILED))
            return new Timestamp(System.currentTimeMillis());

        //query
        Class<CollectionRequest> reqType = CollectionRequest.class;
        EntityManager entityManager = collectionRequestReaderRepository.getEntityManager();//FIXME: appropriate to use reader repo here?

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Timestamp> query = builder.createQuery(Timestamp.class);
        Root<CollectionRequest> reqTable = query.from(reqType);
        query
                .select(builder.least(reqTable.get("requestedTime").as(Timestamp.class)))
                .where(builder.and(
                        builder.equal(reqTable.get("vendor"), vendor),
                        builder.equal(reqTable.get("status"), status)));
        Timestamp ts = entityManager.createQuery(query).getSingleResult();
        if (ts == null)
            ts = new Timestamp(System.currentTimeMillis());

        return ts;
    }

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

    class ReqCmp implements Comparator<CollectionRequest> {
        public int compare(CollectionRequest lhs, CollectionRequest rhs) {
            int ret = lhs.getDomain().compareTo(rhs.getDomain());
            if (ret == 0)
                ret = lhs.getRequestedTime().compareTo(rhs.getRequestedTime());

            return ret;
        }
    }
}
