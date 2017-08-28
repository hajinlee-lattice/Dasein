package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.pls.dao.RatingEngineDao;
import com.latticeengines.pls.dao.RuleBasedModelDao;
import com.latticeengines.pls.entitymanager.RuleBasedModelEntityMgr;

@Component("ruleBasedModelEntityMgr")
public class RuleBasedModelEntityMgrImpl extends BaseEntityMgrImpl<RuleBasedModel> implements RuleBasedModelEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelEntityMgrImpl.class);

    @Autowired
    private RuleBasedModelDao ruleBasedModelDao;

    @Autowired
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
                return retrievedRuleBasedModel;
            }
        }
    }

    private void updateExistingRuleBasedModel(RuleBasedModel retrievedRuleBasedModel, RuleBasedModel ruleBasedModel,
            String ratingEngineId) {
        log.info(String.format("Updating a rule based model with id %s for ratingEngine %s", ruleBasedModel.getId(),
                ratingEngineId));
        if (ruleBasedModel.getRatingRule() != null) {
            retrievedRuleBasedModel.setRatingRule(ruleBasedModel.getRatingRule());
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
