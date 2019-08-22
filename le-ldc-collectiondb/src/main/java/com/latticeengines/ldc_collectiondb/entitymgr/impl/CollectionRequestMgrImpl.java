package com.latticeengines.ldc_collectiondb.entitymgr.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entitymgr.CollectionRequestMgr;
import com.latticeengines.ldc_collectiondb.repository.reader.CollectionRequestReaderRepository;
import com.latticeengines.ldc_collectiondb.repository.writer.CollectionRequestWriterRepository;

@Component
public class CollectionRequestMgrImpl extends JpaEntityMgrRepositoryImpl<CollectionRequest, Long> implements CollectionRequestMgr {

    private static final int RAW_REQ_BATCH = 16;

    @Inject
    private CollectionRequestWriterRepository repository;

    @Inject
    private CollectionRequestReaderRepository readerRepository;

    @Override
    public BaseJpaRepository<CollectionRequest, Long> getRepository() {
        return repository;
    }

    @Override
    public List<CollectionRequest> getByVendorAndDomains(String vendor, Collection<String> domains) {
        return readerRepository.findByVendorAndDomainIn(vendor, domains);
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
        List<CollectionRequest> resultList = readerRepository
                .findByVendorAndStatusOrderByRequestedTimeAsc(vendor, CollectionRequest.STATUS_READY,
                        PageRequest.of(0, upperLimit));

        return resultList;

    }

    @Override
    public List<CollectionRequest> getPending(String vendor, List<CollectionWorker> finishedWorkers) {

        List<String> finishedWorkerIds = new ArrayList<>(finishedWorkers.size());
        for (CollectionWorker finishedWorker : finishedWorkers) {

            finishedWorkerIds.add(finishedWorker.getWorkerId());

        }

        List<String> excluedStatus = Arrays.asList(
                CollectionRequest.STATUS_DELIVERED,
                CollectionRequest.STATUS_FAILED);
        return readerRepository.findByVendorAndPickupWorkerInAndStatusNotIn(vendor, finishedWorkerIds, excluedStatus);

    }

    @Override
    public List<CollectionRequest> getDelivered(String pickupWorker) {

        return readerRepository.findByPickupWorker(pickupWorker);

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

        List<CollectionRequest> resultList = readerRepository
                .findByVendorAndStatusOrderByRequestedTimeAsc(vendor, status, PageRequest.of(0, 1));

        if (resultList.size() == 0) {

            return new Timestamp(System.currentTimeMillis());

        }

        Timestamp ts = resultList.get(0).getRequestedTime();
        resultList.clear();

        return ts;

    }

    @Override
    public void cleanupRequestBetween(Timestamp start, Timestamp end) {
        repository.removeByRequestedTimeBetween(start, end);
    }

    @Override
    public void cleanupRequests(Collection<String> statuses, String vendor, Timestamp before, int batch) {

        Long minPid = repository.getMinPid(vendor);
        if (minPid == null) {

            return;

        }

        repository.removeByStatusInAndVendorAndDeliveryTimeBeforeAndPidLessThan(statuses, vendor, before, minPid + batch);

    }

    @Override
    @Transactional
    public void saveRequests(Collection<CollectionRequest> requests) {

        int batch = 512;
        if (requests.size() <= batch) {

            repository.saveAll(requests);
            return;

        }

        List<CollectionRequest> reqBuf = new ArrayList<>(batch);
        for (CollectionRequest req: requests) {

            reqBuf.add(req);

            if (reqBuf.size() == batch) {

                repository.saveAll(reqBuf);
                reqBuf.clear();

            }

        }

        if (reqBuf.size() > 0) {

            repository.saveAll(reqBuf);
            reqBuf.clear();

        }

    }
}
