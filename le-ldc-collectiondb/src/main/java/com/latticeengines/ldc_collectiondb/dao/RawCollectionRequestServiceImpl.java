package com.latticeengines.ldc_collectiondb.dao;

import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.repository.RawCollectionRequestRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;
import java.util.BitSet;
import java.util.List;

@Component
public class RawCollectionRequestServiceImpl implements RawCollectionRequestService {
    @Autowired
    RawCollectionRequestRepository rawCollectionRequestRepository;

    @Transactional
    public RawCollectionRequest getById(long id) {
        return rawCollectionRequestRepository.findById(id).get();
    }

    @Transactional
    public void save(RawCollectionRequest rawReq) {
        rawCollectionRequestRepository.save(rawReq);
    }

    @Transactional
    public void update(RawCollectionRequest rawReq) {
        rawCollectionRequestRepository.save(rawReq);
    }

    @Transactional
    public void delete(long id) {
        rawCollectionRequestRepository.deleteById(id);
    }

    @Transactional
    public List<RawCollectionRequest> getAll() {
        return rawCollectionRequestRepository.findAll();
    }

    @Transactional
    public List<RawCollectionRequest> getNonTransferred() {
        Class<RawCollectionRequest> reqType = RawCollectionRequest.class;
        EntityManager entityManager = rawCollectionRequestRepository.getEntityManager();
        EntityType<RawCollectionRequest> reqEntityType = entityManager.getMetamodel().entity(reqType);

        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<RawCollectionRequest> criteriaQuery = criteriaBuilder.createQuery(reqType);
        Root<RawCollectionRequest> fromClause = criteriaQuery.from(reqType);
        criteriaQuery.where(criteriaBuilder.equal(fromClause.get("transferred"), false));
        TypedQuery<RawCollectionRequest> typedQuery = entityManager.createQuery(criteriaQuery);
        List<RawCollectionRequest> resultList = typedQuery.getResultList();

        return resultList;
    }

    @Transactional
    public void updateTransferredStatus(List<RawCollectionRequest> added, BitSet filter, boolean deleteFiltered) {
        //update transferred status
        for (int i = 0; i < added.size(); ++i)
        {
            if (filter.get(i))
                continue;

            added.get(i).setTransferred(true);

            rawCollectionRequestRepository.save(added.get(i));
        }

        //delete filtered items?
        if (!deleteFiltered)
            return;
        for (int i = 0; i < added.size(); ++i)
        {
            if (!filter.get(i))
                continue;

            rawCollectionRequestRepository.delete(added.get(i));
        }
    }
}
