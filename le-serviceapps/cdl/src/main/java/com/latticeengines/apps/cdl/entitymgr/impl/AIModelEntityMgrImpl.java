package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.AIModelDao;
import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitable;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitor;
import com.latticeengines.apps.cdl.repository.writer.AIModelRepository;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;

@Component("aiModelEntityMgr")
public class AIModelEntityMgrImpl extends BaseEntityMgrRepositoryImpl<AIModel, Long> //
        implements AIModelEntityMgr, GraphVisitable {

    private static final Logger log = LoggerFactory.getLogger(AIModelEntityMgrImpl.class);

    @Inject
    private AIModelRepository aiModelRepository;

    @Inject
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
    public List<AIModel> findAllByRatingEngineId(String ratingEngineId) {
        return aiModelRepository.findAllByRatingEngineId(ratingEngineId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AIModel> findAllByRatingEngineId(String ratingEngineId, Pageable pageable) {
        if (pageable == null) {
            return aiModelRepository.findAllByRatingEngineId(ratingEngineId);
        }
        return aiModelRepository.findAllByRatingEngineId(ratingEngineId, pageable);
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
    public AIModel createAIModel(AIModel aiModel, String ratingEngineId) {
        log.info(String.format("Creating an AI model with id %s for ratingEngine %s", aiModel.getId(), ratingEngineId));
        getDao().create(aiModel);
        return aiModel;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public AIModel updateAIModel(AIModel aiModel, AIModel retrievedAIModel, String ratingEngineId) {
        updateExistingAIModel(retrievedAIModel, aiModel, ratingEngineId);
        getDao().update(retrievedAIModel);
        return retrievedAIModel;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MetadataSegment inflateParentSegment(AIModel aiModel) {
        return aiModelDao.findParentSegmentById(aiModel.getId());
    }

    private void updateExistingAIModel(AIModel retrievedAIModel, AIModel aiModel, String ratingEngineId) {
        log.info(String.format("Updating AI Model with id %s for ratingEngine %s", aiModel.getId(), ratingEngineId));
        retrievedAIModel.setPredictionType(aiModel.getPredictionType());
        retrievedAIModel.setTrainingSegment(aiModel.getTrainingSegment());
        retrievedAIModel.setModelingJobId(
                aiModel.getModelingYarnJobId() != null ? aiModel.getModelingYarnJobId().toString() : null);
        retrievedAIModel.setModelingJobStatus(aiModel.getModelingJobStatus());
        retrievedAIModel.setModelSummaryId(aiModel.getModelSummaryId());
        retrievedAIModel.setPythonMajorVersion(aiModel.getPythonMajorVersion());
        retrievedAIModel.getAdvancedModelingConfig().copyConfig(aiModel.getAdvancedModelingConfig());
    }

    @Override
    public Set<Triple<String, String, String>> extractDependencies(AIModel aiModel) {
        Set<Triple<String, String, String>> attrDepSet = null;
        if (aiModel != null && aiModel.getTrainingSegment() != null) {
            attrDepSet = new HashSet<Triple<String, String, String>>();
            attrDepSet.add(ParsedDependencies.tuple(aiModel.getTrainingSegment().getName(), //
                    VertexType.SEGMENT, EdgeType.DEPENDS_ON_FOR_TRAINING));
        }
        if (CollectionUtils.isNotEmpty(attrDepSet)) {
            log.info(String.format("Extracted dependencies from ai based model %s: %s", aiModel.getId(),
                    JsonUtils.serialize(attrDepSet)));
        }
        return attrDepSet;
    }

    @Override
    public boolean shouldSkipTenantDependency() {
        return true;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public void accept(GraphVisitor visitor, Object entity) throws Exception {
        entity = findById(((AIModel) entity).getId());
        visitor.visit((AIModel) entity, parse((AIModel) entity, null));
    }
}
