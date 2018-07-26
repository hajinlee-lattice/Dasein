package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.AIModelDao;
import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.AIModelRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.AIModel;

@Component("aiModelEntityMgr")
public class AIModelEntityMgrImpl extends BaseEntityMgrRepositoryImpl<AIModel, Long>
        implements AIModelEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(AIModelEntityMgrImpl.class);

    @Autowired
    private AIModelRepository aiModelRepository;

    @Autowired
    private AIModelDao aiModelDao;

    @Override
    public BaseJpaRepository<AIModel, Long> getRepository() {
        return aiModelRepository;
    }

    @Override
    public BaseDao<AIModel> getDao() {
        return aiModelDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AIModel findById(String id) {
        return aiModelRepository.findById(id);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AIModel> findByRatingEngineId(String ratingEngineId, Pageable pageable) {
        if (pageable == null) {
            return aiModelRepository.findByRatingEngineId(ratingEngineId);
        }
        return aiModelRepository.findByRatingEngineId(ratingEngineId, pageable);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteById(String id) {
        AIModel entity = findById(id);
        if (entity == null) {
            throw new NullPointerException(String.format("AIModel with id %s cannot be found", id));
        }
        super.delete(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public AIModel createOrUpdateAIModel(AIModel aiModel, String ratingEngineId) {
        if (aiModel.getId() == null) {
            aiModel.setId(AIModel.generateIdStr());
            log.info(String.format("Creating an AI model with id %s for ratingEngine %s",
                    aiModel.getId(), ratingEngineId));
            getDao().create(aiModel);
            return aiModel;
        } else {
            AIModel retrievedAIModel = findById(aiModel.getId());
            if (retrievedAIModel == null) {
                log.warn(String.format("AIModel with id %s is not found. Creating a new one",
                        aiModel.getId()));
                getDao().create(aiModel);
                return aiModel;
            } else {
                updateExistingAIModel(retrievedAIModel, aiModel, ratingEngineId);
                getDao().update(retrievedAIModel);
                return retrievedAIModel;
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MetadataSegment inflateParentSegment(AIModel aiModel) {
        return aiModelDao.findParentSegmentById(aiModel.getId());
    }

    private void updateExistingAIModel(AIModel retrievedAIModel, AIModel aiModel,
            String ratingEngineId) {
        log.info(String.format("Updating AI Model with id %s for ratingEngine %s", aiModel.getId(),
                ratingEngineId));
        retrievedAIModel.setPredictionType(aiModel.getPredictionType());
        retrievedAIModel.setTrainingSegment(aiModel.getTrainingSegment());
        retrievedAIModel.setModelingJobId(
                aiModel.getModelingYarnJobId() != null ? aiModel.getModelingYarnJobId().toString()
                        : null);
        retrievedAIModel.setModelingJobStatus(aiModel.getModelingJobStatus());
        retrievedAIModel.setModelSummaryId(aiModel.getModelSummaryId());
        retrievedAIModel.getAdvancedModelingConfig()
                .copyConfig(aiModel.getAdvancedModelingConfig());
    }
}
