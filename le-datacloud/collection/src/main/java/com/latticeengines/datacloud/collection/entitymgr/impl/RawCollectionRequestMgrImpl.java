package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.sql.Timestamp;
import java.util.BitSet;
import java.util.List;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RawCollectionRequestMgr;
import com.latticeengines.datacloud.collection.repository.RawCollectionRequestRepository;
import com.latticeengines.datacloud.collection.repository.reader.RawCollectionRequestReaderRepository;
import com.latticeengines.datacloud.collection.util.CollectionDBUtil;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

@Component
public class RawCollectionRequestMgrImpl extends JpaEntityMgrRepositoryImpl<RawCollectionRequest, Long> implements RawCollectionRequestMgr {
    private static final Logger log = LoggerFactory.getLogger(RawCollectionRequestMgrImpl.class);

    @Inject
    private RawCollectionRequestRepository rawCollectionRequestRepository;
    @Inject
    private RawCollectionRequestReaderRepository rawCollectionRequestReaderRepository;

    @Override
    public BaseJpaRepository<RawCollectionRequest, Long> getRepository() {
        return rawCollectionRequestRepository;
    }

    public boolean addNewDomains(List<String> domains, String vendor, String reqId) {
        String vendorUpper = vendor.toUpperCase();
        if (!CollectionDBUtil.getVendors().contains(vendorUpper)) {
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

            rawCollectionRequestRepository.save(req);
        }

        return true;
    }

    public List<RawCollectionRequest> getNonTransferred() {
        Class<RawCollectionRequest> reqType = RawCollectionRequest.class;
        EntityManager entityManager = rawCollectionRequestReaderRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<RawCollectionRequest> query = builder.createQuery(reqType);
        Root<RawCollectionRequest> reqTable = query.from(reqType);
        query.select(reqTable).where(builder.equal(reqTable.get("transferred"), false));
        TypedQuery<RawCollectionRequest> typedQuery = entityManager.createQuery(query);
        List<RawCollectionRequest> resultList = typedQuery.getResultList();

        return resultList;
    }

    public List<RawCollectionRequest> getNonTransferred(String vendor) {
        //check vendor
        String vendor_norm = vendor.toUpperCase();
        if (!CollectionDBUtil.getVendors().contains(vendor_norm))
            return null;

        Class<RawCollectionRequest> reqType = RawCollectionRequest.class;
        EntityManager entityManager = rawCollectionRequestReaderRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<RawCollectionRequest> query = builder.createQuery(reqType);
        Root<RawCollectionRequest> reqTable = query.from(reqType);
        query.select(reqTable).where(builder.and(builder.equal(reqTable.get("vendor"), vendor_norm), builder.equal(reqTable.get("transferred"), false)));
        TypedQuery<RawCollectionRequest> typedQuery = entityManager.createQuery(query);
        List<RawCollectionRequest> resultList = typedQuery.getResultList();

        return resultList;
    }

    public void updateTransferredStatus(List<RawCollectionRequest> added, BitSet filter, boolean deleteFiltered) {
        //update transferred status
        for (int i = 0; i < added.size(); ++i) {
            if (filter.get(i))
                continue;

            added.get(i).setTransferred(true);

            rawCollectionRequestRepository.save(added.get(i));
        }

        //delete filtered items?
        if (!deleteFiltered)
            return;
        for (int i = 0; i < added.size(); ++i) {
            if (!filter.get(i))
                continue;

            rawCollectionRequestRepository.delete(added.get(i));
        }
    }

}
