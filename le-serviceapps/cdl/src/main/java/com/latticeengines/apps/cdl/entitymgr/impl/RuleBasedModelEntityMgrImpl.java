package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.RatingEngineDao;
import com.latticeengines.apps.cdl.dao.RuleBasedModelDao;
import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("ruleBasedModelEntityMgr")
public class RuleBasedModelEntityMgrImpl extends BaseEntityMgrImpl<RuleBasedModel> implements RuleBasedModelEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelEntityMgrImpl.class);

    @Inject
    private RuleBasedModelDao ruleBasedModelDao;

    @Inject
    private RatingEngineDao ratingEngineDao;

    @Override
    public BaseDao<RuleBasedModel> getDao() {
        return ruleBasedModelDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public RuleBasedModel createRuleBasedModel(RuleBasedModel ruleBasedModel, String ratingEngineId) {
        log.info(String.format("Creating a rule based model with id %s for ratingEngine %s", ruleBasedModel.getId(),
                ratingEngineId));
        ruleBasedModelDao.create(ruleBasedModel);
        return ruleBasedModel;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public RuleBasedModel updateRuleBasedModel(RuleBasedModel ruleBasedModel, RuleBasedModel retrievedRuleBasedModel,
            String ratingEngineId) {
        updateExistingRuleBasedModel(retrievedRuleBasedModel, ruleBasedModel, ratingEngineId);
        ruleBasedModelDao.update(retrievedRuleBasedModel);
        // set Rule-based Rating Action Context
        RatingEngine ratingEngine = ratingEngineDao.findById(ratingEngineId);
        if (ruleBasedModel.getRatingRule() != null && !ratingEngine.getJustCreated()) {
            setRuleChangeActionContext(ratingEngineId, retrievedRuleBasedModel);
        }
        return retrievedRuleBasedModel;
    }

    private void setRuleChangeActionContext(String ratingEngineId, RuleBasedModel retrievedRuleBasedModel) {
        log.info(String.format("Set Rule Change Action Context for Rating Model %s, Rating Engine %s",
                retrievedRuleBasedModel.getId(), ratingEngineId));
        Action ruleChangeAction = new Action();
        ruleChangeAction.setType(ActionType.RATING_ENGINE_CHANGE);
        RatingEngine ratingEngine = ratingEngineDao.findById(ratingEngineId);
        ruleChangeAction.setActionInitiator(ratingEngine.getCreatedBy());
        RatingEngineActionConfiguration reActionConfig = new RatingEngineActionConfiguration();
        reActionConfig.setModelId(retrievedRuleBasedModel.getId());
        reActionConfig.setRatingEngineId(ratingEngineId);
        reActionConfig.setSubType(RatingEngineActionConfiguration.SubType.RULE_MODEL_BUCKET_CHANGE);
        ruleChangeAction.setActionConfiguration(reActionConfig);
        ActionContext.setAction(ruleChangeAction);
    }

    private void updateExistingRuleBasedModel(RuleBasedModel retrievedRuleBasedModel, RuleBasedModel ruleBasedModel,
            String ratingEngineId) {
        log.info(String.format("Updating a rule based model with id %s for ratingEngine %s", ruleBasedModel.getId(),
                ratingEngineId));
        if (ruleBasedModel.getRatingRule() != null) {
            retrievedRuleBasedModel.setRatingRule(ruleBasedModel.getRatingRule());
        }
        if (ruleBasedModel.getSelectedAttributes() != null) {
            retrievedRuleBasedModel.setSelectedAttributes(ruleBasedModel.getSelectedAttributes());
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RuleBasedModel> findAllByRatingEngineId(String ratingEngineid) {
        RatingEngine ratingEngine = ratingEngineDao.findById(ratingEngineid);
        if (ratingEngine == null || ratingEngine.getPid() == null) {
            return Collections.emptyList();
        }
        return ruleBasedModelDao.findAllByField("FK_RATING_ENGINE_ID", ratingEngine.getPid());
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RuleBasedModel findById(String id) {
        return ruleBasedModelDao.findById(id);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void deleteById(String id) {
        RuleBasedModel entity = findById(id);
        if (entity == null) {
            throw new NullPointerException(String.format("RuleBasedModel with id %s cannot be found", id));
        }
        super.delete(entity);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void deleteRuleBasedModel(RuleBasedModel ruleBasedModel) {
        if (ruleBasedModel == null) {
            throw new NullPointerException("RuleBasedModel cannot be found");
        }
        super.delete(ruleBasedModel);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MetadataSegment inflateParentSegment(RuleBasedModel ruleBasedModel) {
        return ruleBasedModelDao.findParentSegmentById(ruleBasedModel.getId());
    }

    @Override
    public Set<Triple<String, String, String>> extractDependencies(RuleBasedModel ruleBasedModel) {
        Set<Triple<String, String, String>> attrDepSet = new HashSet<Triple<String, String, String>>();
        Set<AttributeLookup> attributeLookups = ruleBasedModel.getRatingModelAttributes();

        if (CollectionUtils.isNotEmpty(attributeLookups)) {
            attributeLookups.stream() //
                    .forEach(attributeLookup -> {
                        if (attributeLookup.getEntity() == BusinessEntity.Rating) {
                            attrDepSet.add(ParsedDependencies.tuple(
                                    attributeLookup.getEntity() + "." + attributeLookup.getAttribute(), //
                                    VertexType.RATING_ATTRIBUTE, EdgeType.DEPENDS_ON));
                        }
                    });
        }
        return attrDepSet;
    }

    @Override
    public boolean shouldSkipTenantDependency() {
        return true;
    }
}
