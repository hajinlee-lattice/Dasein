package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.CollectionWorkerMgr;
import com.latticeengines.datacloud.collection.repository.CollectionWorkerRepository;
import com.latticeengines.datacloud.collection.util.CollectionDBUtil;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;

@Component
public class CollectionWorkerMgrImpl extends JpaEntityMgrRepositoryImpl<CollectionWorker, Long> implements CollectionWorkerMgr {
    private static final HashSet<String> workerStatusSet = new HashSet<String>();

    static {
        workerStatusSet.add(CollectionWorker.STATUS_NEW);
        workerStatusSet.add(CollectionWorker.STATUS_RUNNING);
        workerStatusSet.add(CollectionWorker.STATUS_FINISHED);
        workerStatusSet.add(CollectionWorker.STATUS_CONSUMED);
    }

    @Inject
    CollectionWorkerRepository collectionWorkerRepository;

    @Override
    public BaseJpaRepository<CollectionWorker, Long> getRepository() {
        return collectionWorkerRepository;
    }

    public List<CollectionWorker> getWorkerByStatus(List<String> statusList) {
        //check
        for (int i = 0; i < statusList.size(); ++i) {
            String status = statusList.get(i);

            if (!status.equals(CollectionWorker.STATUS_NEW) &&
                    !status.equals(CollectionWorker.STATUS_RUNNING) &&
                    !status.equals(CollectionWorker.STATUS_FINISHED) &&
                    !status.equals(CollectionWorker.STATUS_CONSUMED) &&
                    !status.equals(CollectionWorker.STATUS_FAILED))
                return null;
        }

        //query
        Class<CollectionWorker> reqType = CollectionWorker.class;
        EntityManager entityManager = collectionWorkerRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CollectionWorker> query = builder.createQuery(reqType);
        Root<CollectionWorker> reqTable = query.from(reqType);
        query
                .select(reqTable)
                .where(reqTable.get("status").in(statusList));
        TypedQuery<CollectionWorker> typedQuery = entityManager.createQuery(query);
        List<CollectionWorker> resultList = typedQuery.getResultList();

        return resultList;
    }

    public List<CollectionWorker> getWorkerStopped(String vendor, Timestamp after) {
        //check
        vendor = vendor.toUpperCase();
        if (!CollectionDBUtil.getVendors().contains(vendor))
            return null;

        //query
        Class<CollectionWorker> reqType = CollectionWorker.class;
        EntityManager entityManager = collectionWorkerRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CollectionWorker> query = builder.createQuery(reqType);
        Root<CollectionWorker> reqTable = query.from(reqType);
        query.select(reqTable).where(builder.and(builder.greaterThanOrEqualTo(reqTable.get("spawnTime"), after),
                builder.or(
                        builder.equal(reqTable.get("status"), CollectionWorker.STATUS_CONSUMED),
                        builder.equal(reqTable.get("status"), CollectionWorker.STATUS_FAILED))));
        TypedQuery<CollectionWorker> typedQuery = entityManager.createQuery(query);
        List<CollectionWorker> resultList = typedQuery.getResultList();

        return resultList;
    }

    public int getActiveWorkerCount(String vendor) {
        //check
        vendor = vendor.toUpperCase();
        if (!CollectionDBUtil.getVendors().contains(vendor))
            return Integer.MAX_VALUE;

        Class<CollectionWorker> reqType = CollectionWorker.class;
        EntityManager entityManager = collectionWorkerRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> query = builder.createQuery(Long.class);
        Root<CollectionWorker> reqTable = query.from(reqType);
        query
                .select(builder.count(reqTable))
                .where(builder.and(
                        builder.equal(reqTable.get("vendor"), vendor),
                        builder.or(
                                builder.equal(reqTable.get("status"), CollectionWorker.STATUS_NEW),
                                builder.equal(reqTable.get("status"), CollectionWorker.STATUS_RUNNING))));
        return entityManager.createQuery(query).getSingleResult().intValue();
    }

    /*
    public void updateStatus(String taskArn, String status)
    {
        //check status
        status = status.toUpperCase();
        if (!workerStatusSet.contains(status))
            return;

        Class<CollectionWorker> reqType = CollectionWorker.class;
        EntityManager entityManager = collectionWorkerRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CollectionWorker> query = builder.createQuery(reqType);
        Root<CollectionWorker> reqTable = query.from(reqType);
        query.select(reqTable).where(builder.equal(reqTable.get("taskArn"), taskArn));
        TypedQuery<CollectionWorker> typedQuery = entityManager.createQuery(query);
        CollectionWorker result = typedQuery.getSingleResult();

        result.setStatus(status);
        collectionWorkerRepository.save(result);
    }

    public int cleanOutdated(Timestamp before) {
        //query
        Class<CollectionWorker> reqType = CollectionWorker.class;
        EntityManager entityManager = collectionWorkerRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaDelete<CollectionWorker> delete = builder.createCriteriaDelete(reqType);
        Root<CollectionWorker> reqTable = delete.from(reqType);//query.from(reqType);
        delete.where(builder.and(builder.isNotNull(reqTable.get("terminationTime")), builder.lessThan(reqTable.get("terminationTime"), before)));

        entityManager.getTransaction().begin();
        int ret = entityManager.createQuery(delete).executeUpdate();
        entityManager.getTransaction().commit();

        return ret;
    }

    public int updateNewWorkerStatus(List<String> activeTaskArns) {
        Class<CollectionWorker> reqType = CollectionWorker.class;
        EntityManager entityManager = collectionWorkerRepository.getEntityManager();

        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaUpdate<CollectionWorker> update = builder.createCriteriaUpdate(reqType);
        Root<CollectionWorker> reqTable = update.from(reqType);
        update
                .set(reqTable.get("status"), CollectionWorker.STATUS_RUNNING)
                .where(builder.and(
                        builder.equal(reqTable.get("status"), CollectionWorker.STATUS_NEW),
                        reqTable.get("taskArn").in(activeTaskArns)
                ));

        entityManager.getTransaction().begin();
        int ret = entityManager.createQuery(update).executeUpdate();
        entityManager.getTransaction().commit();

        return ret;
    }*/
}
