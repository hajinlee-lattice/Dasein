package com.latticeengines.apps.lp.entitymgr.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.lp.dao.BucketMetadataDao;
import com.latticeengines.apps.lp.entitymgr.BucketMetadataEntityMgr;
import com.latticeengines.apps.lp.repository.reader.BucketMetadataReaderRepository;
import com.latticeengines.apps.lp.repository.writer.AIModelRepository;
import com.latticeengines.apps.lp.repository.writer.BucketMetadataWriterRepository;
import com.latticeengines.apps.lp.repository.writer.ModelSummaryWriterRepository;
import com.latticeengines.apps.lp.repository.writer.RatingEngineReository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;

@Component("bucketMetadataEntityMgr")
public class BucketMetadataEntityMgrImpl extends BaseEntityMgrRepositoryImpl<BucketMetadata, Long>
        implements BucketMetadataEntityMgr {

    @Inject
    private BucketMetadataWriterRepository repository;

    @Inject
    private BucketMetadataReaderRepository readerRepository;

    @Inject
    private BucketMetadataDao dao;

    @Inject
    private ModelSummaryWriterRepository modelSummaryRepository;

    @Inject
    private RatingEngineReository ratingEngineRepository;

    @Inject
    private AIModelRepository aiModelRepostiry;

    @Override
    public BaseDao<BucketMetadata> getDao() {
        return dao;
    }

    @Override
    public BaseJpaRepository<BucketMetadata, Long> getRepository() {
        return repository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createBucketMetadata(List<BucketMetadata> bucketMetadataList, String modelGuid, String engineId) {
        ModelSummary modelSummary = modelSummaryRepository.findById(modelGuid);
        RatingEngine ratingEngine = ratingEngineRepository.findById(engineId);
        bucketMetadataList.forEach(bucketMetadata -> {
            bucketMetadata.setModelSummary(modelSummary);
            bucketMetadata.setRatingEngine(ratingEngine);
            dao.create(bucketMetadata);
        });
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getBucketMetadatasForModelFromReader(String modelGuid) {
        return readerRepository.findByModelSummary_Id(modelGuid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getBucketMetadatasForEngineFromReader(String engineId) {
        return readerRepository.findByRatingEngine_Id(engineId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getUpToDateBucketMetadatasForModelFromReader(String modelId) {
        BucketMetadata bm = readerRepository.findFirstByModelSummary_IdOrderByCreationTimestampDesc(modelId);
        if (bm == null) {
            return Collections.emptyList();
        } else {
            return readerRepository.findByCreationTimestampAndModelSummary_Id(bm.getCreationTimestamp(), modelId);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getUpToDateBucketMetadatasForEngineFromReader(String engineId) {
        RatingEngine ratingEngine = ratingEngineRepository.findById(engineId);

        AIModel aiModel = aiModelRepostiry.findByPid(ratingEngine.getLatestIteration().getPid());
        return getUpToDateBucketMetadatasForModelFromReader(aiModel.getModelSummaryId());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public BucketMetadata getBucketMetadatasByBucketNameAndTimestamp(String bucketName, long timestamp) {
        return repository.findByCreationTimestampAndBucketName(timestamp, BucketName.fromValue(bucketName));
    }

}
