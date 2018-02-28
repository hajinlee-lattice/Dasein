package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.pls.dao.BucketMetadataDao;
import com.latticeengines.pls.entitymanager.BucketMetadataEntityMgr;
import com.latticeengines.pls.repository.BucketMetadataRepository;

@Component("bucketMetadataEntityMgr")
public class BucketMetadataEntityMgrImpl extends BaseEntityMgrRepositoryImpl<BucketMetadata, Long>
        implements BucketMetadataEntityMgr {

    @Inject
    private BucketMetadataRepository bucketMetadataRepository;

    @Inject
    private BucketMetadataDao bucketMetadataDao;

    @Override
    public BaseDao<BucketMetadata> getDao() {
        return bucketMetadataDao;
    }

    @Override
    public BaseJpaRepository<BucketMetadata, Long> getRepository() {
        return bucketMetadataRepository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getBucketMetadatasForModelId(String modelId) {
        return bucketMetadataDao.findBucketMetadatasForModelId(modelId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getUpToDateBucketMetadatasForModelId(String modelId) {
        return bucketMetadataDao.findUpToDateBucketMetadatasForModelId(modelId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getBucketMetadatasBasedOnRatingEngine(RatingEngine ratingEngine) {
        return bucketMetadataRepository.findByRatingEngine(ratingEngine);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getUpToDateBucketMetadatasBasedOnRatingEngine(RatingEngine ratingEngine) {
        return bucketMetadataDao.findUpToDateBucketMetadatasForRatingEngine(ratingEngine.getPid());
    }

}
