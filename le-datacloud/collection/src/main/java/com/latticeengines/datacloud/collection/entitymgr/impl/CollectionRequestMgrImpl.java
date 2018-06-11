package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.CollectionRequestMgr;
import com.latticeengines.datacloud.collection.repository.CollectionRequestRepository;
import com.latticeengines.datacloud.collection.repository.reader.CollectionRequestReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;

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

    @Override
    public List<CollectionRequest> getByVendorAndDomains(String vendor, Collection<String> domains) {
        return collectionRequestReaderRepository.findByVendorAndDomainIn(vendor, domains);
    }

    @Override
    public List<CollectionRequest> getReady(String vendor, int upperLimit) {
        //query
        //select * from CollectionRequest where VENDOR = vendor and STATUS = 'READY' order by REQUESTED_TIME asc;
        /*
        Class<CollectionRequest> reqType = CollectionRequest.class;
        EntityManager entityManager = collectionRequestReaderRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CollectionRequest> query = builder.createQuery(reqType);
        Root<CollectionRequest> reqTable = query.from(reqType);
        query.select(reqTable).where(builder.and(builder.equal(reqTable.get("vendor"), vendor), builder.equal(reqTable.get("status"), CollectionRequest.STATUS_READY))).orderBy(builder.asc(reqTable.get("requestedTime")));
        TypedQuery<CollectionRequest> typedQuery = entityManager.createQuery(query);
        List<CollectionRequest> resultList = typedQuery.setMaxResults(upperLimit).getResultList();*/
        List<CollectionRequest> resultList = collectionRequestReaderRepository
                .findByVendorAndStatusOrderByRequestedTimeAsc(vendor, CollectionRequest.STATUS_READY);
        if (resultList.size() > upperLimit)
            resultList = resultList.subList(0, upperLimit);

        return resultList;
    }

    @Override
    public List<CollectionRequest> getPending(String vendor, List<CollectionWorker> finishedWorkers) {
        List<String> finishedWorkerIds = new ArrayList<>(finishedWorkers.size());
        for (int i = 0; i < finishedWorkers.size(); ++i)
            finishedWorkerIds.add(finishedWorkers.get(i).getWorkerId());

        List<String> excluedStatus = new ArrayList<>(2);
        excluedStatus.add(CollectionRequest.STATUS_DELIVERED);
        excluedStatus.add(CollectionRequest.STATUS_FAILED);
        return collectionRequestReaderRepository.findByVendorAndPickupWorkerInAndStatusNotIn(vendor,
                finishedWorkerIds,
                excluedStatus);
    }

    @Override
    public List<CollectionRequest> getDelivered(String pickupWorker) {
        return collectionRequestReaderRepository.findByPickupWorker(pickupWorker);
    }

    @Override
    public Timestamp getEarliestTime(String vendor, String status) {
        /*
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
        Timestamp ts = entityManager.createQuery(query).getSingleResult();*/

        List<CollectionRequest> resultList = collectionRequestReaderRepository
                .findByVendorAndStatusOrderByRequestedTimeAsc(vendor, status);

        if (resultList.size() == 0)
            return new Timestamp(System.currentTimeMillis());

        Timestamp ts = resultList.get(0).getRequestedTime();
        resultList.clear();

        return ts;
    }
}
