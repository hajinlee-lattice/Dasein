package com.latticeengines.ldc_collectiondb.dao;

import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.repository.CollectionRequestRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.EntityType;
import java.util.*;

@Component
public class CollectionRequestServiceImpl implements CollectionRequestService {
    @Autowired
    CollectionRequestRepository collectionRequestRepository;

    @Transactional
    public CollectionRequest getById(long id) {
        return collectionRequestRepository.findById(id).get();
    }

    @Transactional
    public void save(CollectionRequest rawReq) {
        collectionRequestRepository.save(rawReq);
    }

    @Transactional
    public void update(CollectionRequest rawReq) {
        collectionRequestRepository.save(rawReq);
    }

    @Transactional
    public void delete(long id) {
        collectionRequestRepository.deleteById(id);
    }

    @Transactional
    public List<CollectionRequest> getAll() {
        return collectionRequestRepository.findAll();
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

    static final HashMap<String, Integer> vendorCollectionFreq;
    static {
        int monthInSecs = 86400 * 30;
        vendorCollectionFreq = new HashMap<>();
        vendorCollectionFreq.put("ALEXA", monthInSecs * 3);
        vendorCollectionFreq.put("BUILTWITH", monthInSecs * 6);
        vendorCollectionFreq.put("COMPETE", monthInSecs);
        vendorCollectionFreq.put("FEATURE", monthInSecs * 6);
        vendorCollectionFreq.put("HPA_NEW", monthInSecs * 6);
        vendorCollectionFreq.put("ORBINTELLIGENCEV2", monthInSecs * 3);
        vendorCollectionFreq.put("SEMRUSH", monthInSecs);
    }
    static final int microBatch = 16;

    @Transactional
    public BitSet add(List<RawCollectionRequest> toAdd) {
        //sort, first by vendor, then by domain, next by time
        toAdd.sort(new RawRequestCmp());

        //dedup raw requests
        BitSet rawReqFilter = new BitSet(toAdd.size());
        int nextPos = 0;
        for (int i = 0; i < toAdd.size(); i = nextPos)
        {
            nextPos = i + 1;
            String curDomain = toAdd.get(i).getDomain();
            String curVendor = toAdd.get(i).getVendor();

            for (; nextPos < toAdd.size(); ++nextPos)
            {
                RawCollectionRequest nextRawReq = toAdd.get(nextPos);
                int vendorCmpRet = nextRawReq.getVendor().compareTo(curVendor);
                if (vendorCmpRet > 0 || (vendorCmpRet == 0 && nextRawReq.getDomain().compareTo(curDomain) > 0))
                    break;
            }

            for (int k = i + 1; k < nextPos; ++k)
                rawReqFilter.set(k, true);
        }
        if (rawReqFilter.cardinality() == toAdd.size())
            return rawReqFilter;
        //System.out.println("pre filter raw reqs: " + rawReqFilter.cardinality() + " filtered");

        //select * from CollectionRequest where VENDOR = vendor and DOMAIN = domain
        //select * from CollectionRequest where VENDOR in [vendors] and DOMAIN in [domains]
        //select max(REQUESTED_TIME) from CollectionRequest groupby VENDOR, DOMAIN having VENDOR in [vendors] and DOMAIN in [domains]
        int next = 0;
        RawCollectionRequest[] reqBuf = new RawCollectionRequest[microBatch];
        int[] posBuf = new int[microBatch];
        for (int i = 0; i < toAdd.size(); i = next)
        {
            //slide through pre-filtered reqs
            for (; i < toAdd.size() && rawReqFilter.get(i); ++i);
            if (i == toAdd.size())
                break;

            //accumulate micro-batch to process
            int bufLen = 0;
            int j = i;
            for (; j < toAdd.size() && bufLen < microBatch; ++j)
            {
                if (!rawReqFilter.get(j))
                {
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
            //EntityType<CollectionRequest> reqEntityType = entityManager.getMetamodel().entity(reqType);

            CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
            CriteriaQuery<CollectionRequest> criteriaQuery = criteriaBuilder.createQuery(reqType);
            Root<CollectionRequest> fromClause = criteriaQuery.from(reqType);
            List<String> domains = new ArrayList<>();
            for (j = 0; j < bufLen; ++j)
                domains.add(reqBuf[j].getDomain());
            criteriaQuery.where(criteriaBuilder.and(criteriaBuilder.equal(fromClause.get("vendor"), curVendor), fromClause.get("domain").in(domains)));
            TypedQuery<CollectionRequest> typedQuery = entityManager.createQuery(criteriaQuery);
            List<CollectionRequest> resultList = typedQuery.getResultList();

            //sort collection req list
            resultList.sort(new ReqCmp());

            //dedup of same domain
            List<CollectionRequest> reqList = new ArrayList<>();
            for (int k = 0; k < resultList.size();)
            {
                String curDomain = resultList.get(k).getDomain();
                int l = k + 1;
                for (; l < resultList.size(); ++l)
                {
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
            for (int k = 0; k < bufLen && reqPos < reqList.size(); ++k)
            {
                //cur raw req
                RawCollectionRequest curRawReq = reqBuf[k];
                String curDomain = curRawReq.getDomain();

                //slide through collection req list, to find a possible match
                int l = reqPos;
                for (; l < reqList.size(); ++l)
                {
                    if (curDomain.compareTo(reqList.get(l).getDomain()) <= 0)
                        break;
                }
                reqPos = l;

                //determine whether to filter the raw req
                if (l < reqList.size())
                {
                    CollectionRequest curReq = reqList.get(l);
                    if (curDomain.compareTo(curReq.getDomain()) == 0 && (
                            curReq.getStatus().compareTo("COLLECTING") == 0 || curReq.getStatus().compareTo("READY") == 0 || (
                                    curReq.getStatus().compareTo("DELIVERED") == 0 && curRawReq.getRequestedTime().getTime() - curReq.getRequestedTime().getTime() > 1000L * vendorCollectionFreq.get(curVendor)
                                    )
                            ))
                        rawReqFilter.set(posBuf[k], true);
                }
            }
        }

        //insert collection req
        for (int i = 0; i < toAdd.size(); ++i)
        {
            if (rawReqFilter.get(i))
                continue;

            RawCollectionRequest rawReq = toAdd.get(i);

            CollectionRequest colReq = new CollectionRequest();
            colReq.setVendor(rawReq.getVendor());
            colReq.setDomain(rawReq.getDomain());
            colReq.setOriginalRequestId(rawReq.getOriginalRequestId());
            colReq.setRequestedTime(rawReq.getRequestedTime());
            colReq.setRetryAttempts(0);
            colReq.setStatus("READY");
            colReq.setPickupTime(null);
            colReq.setPickupWorker(null);
            colReq.setDeliveryTime(null);

            collectionRequestRepository.save(colReq);
        }

        return rawReqFilter;
    }
}
