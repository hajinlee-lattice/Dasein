package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.AIModelDao;
import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.repository.AIModelRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.AIModel;

@Component("aiModelEntityMgr")
public class AIModelEntityMgrImpl extends BaseEntityMgrRepositoryImpl<AIModel, Long> implements AIModelEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(AIModelEntityMgrImpl.class);

    @Autowired
    private AIModelRepository aiModelRepository;

    @Autowired
    private AIModelDao aiModelDao;

    @Override
    public BaseJpaRepository<AIModel, Long> getRepository() {
        return (BaseJpaRepository<AIModel, Long>) aiModelRepository;
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
            throw new UnsupportedOperationException("Create new AIModel is not Supported");
        } else {
            AIModel retrievedAIModel = findById(aiModel.getId());
            if (retrievedAIModel == null) {
                throw new UnsupportedOperationException("Create new AIModel is not yet Supported");
            } else {
                updateExistingRuleBasedModel(retrievedAIModel, aiModel, ratingEngineId);
                getDao().update(retrievedAIModel);
                return retrievedAIModel;
            }
        }
    }

    private void updateExistingRuleBasedModel(AIModel retrievedAIModel, AIModel aiModel, String ratingEngineId) {
        log.info(String.format("Updating AI Model with id %s for ratingEngine %s", aiModel.getId(), ratingEngineId));

        retrievedAIModel.setModelingMethod(aiModel.getModelingMethod());
        retrievedAIModel.setWorkflowType(aiModel.getWorkflowType());
        retrievedAIModel.setTargetCustomerSet(aiModel.getTargetCustomerSet());
        retrievedAIModel.setTargetProducts(aiModel.getTargetProducts());
        retrievedAIModel.setTrainingProducts(aiModel.getTrainingProducts());
        retrievedAIModel.setTrainingSegment(aiModel.getTrainingSegment());
        retrievedAIModel
                .setModelingJobId(aiModel.getModelingJobId() != null ? aiModel.getModelingJobId().toString() : null);
        retrievedAIModel.setModelingConfigFilters(aiModel.getModelingConfigFilters());
        retrievedAIModel.setModelSummary(aiModel.getModelSummary());
    }

}
