package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

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
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;

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
    @Transactional(propagation = Propagation.REQUIRED)
    public RuleBasedModel createOrUpdateRuleBasedModel(RuleBasedModel ruleBasedModel, String ratingEngineId) {
        if (ruleBasedModel.getId() == null) {
            ruleBasedModel.setId(RuleBasedModel.generateIdStr());
            log.info(String.format("Creating a rule based model with id %s for ratingEngine %s", ruleBasedModel.getId(),
                    ratingEngineId));
            ruleBasedModelDao.create(ruleBasedModel);
            return ruleBasedModel;
        } else {
            RuleBasedModel retrievedRuleBasedModel = findById(ruleBasedModel.getId());
            if (retrievedRuleBasedModel == null) {
                log.warn(String.format("RuleBasedModel with id %s is not found. Creating a new one",
                        ruleBasedModel.getId()));
                ruleBasedModelDao.create(ruleBasedModel);
                return ruleBasedModel;
            } else {
                updateExistingRuleBasedModel(retrievedRuleBasedModel, ruleBasedModel, ratingEngineId);
                ruleBasedModelDao.update(retrievedRuleBasedModel);
                // set Rule-based Rating Action Context
                if (ruleBasedModel.getRatingRule() != null) {
                    setRuleChangeActionContext(ratingEngineId, retrievedRuleBasedModel);
                }
                return retrievedRuleBasedModel;
            }
        }
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
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RuleBasedModel> findAllByRatingEngineId(String ratingEngineid) {
        RatingEngine ratingEngine = ratingEngineDao.findById(ratingEngineid);
        if (ratingEngine == null || ratingEngine.getPid() == null) {
            throw new NullPointerException(
                    String.format("Rating Engine with id of %s cannot be found", ratingEngineid));
        }
        return ruleBasedModelDao.findAllByField("FK_RATING_ENGINE_ID", ratingEngine.getPid());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RuleBasedModel findById(String id) {
        return ruleBasedModelDao.findById(id);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteById(String id) {
        RuleBasedModel entity = findById(id);
        if (entity == null) {
            throw new NullPointerException(String.format("RuleBasedModel with id %s cannot be found", id));
        }
        super.delete(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteRuleBasedModel(RuleBasedModel ruleBasedModel) {
        if (ruleBasedModel == null) {
            throw new NullPointerException("RuleBasedModel cannot be found");
        }
        super.delete(ruleBasedModel);
    }

}
