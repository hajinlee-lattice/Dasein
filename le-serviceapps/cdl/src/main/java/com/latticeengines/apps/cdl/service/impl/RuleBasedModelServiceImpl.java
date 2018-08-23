package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.cdl.service.RuleBasedModelService;
import com.latticeengines.apps.cdl.util.RuleBasedModelDependencyUtil;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;

@Component("ruleBasedModelService")
public class RuleBasedModelServiceImpl extends RatingModelServiceBase<RuleBasedModel> implements RuleBasedModelService {

    protected RuleBasedModelServiceImpl() {
        super(RatingEngineType.RULE_BASED);
    }

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelServiceImpl.class);

    @Inject
    private RuleBasedModelDependencyUtil ruleBasedModelDependencyUtil;

    @Inject
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Override
    public List<RuleBasedModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        return ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
    }

    @Override
    public RuleBasedModel createNewIteration(RuleBasedModel aiModel, RatingEngine ratingEngine) {
        throw new LedpException(LedpCode.LEDP_32001,
                new String[] { "RuleBased Models do not support multiple iterations" });
    }

    @Override
    public RuleBasedModel getRatingModelById(String id) {
        return ruleBasedModelEntityMgr.findById(id);
    }

    @Override
    public RuleBasedModel createOrUpdate(RuleBasedModel ratingModel, String ratingEngineId) {
        log.info(String.format("Creating/Updating a rule based model for Rating Engine %s", ratingEngineId));
        findRatingModelAttributeLookups(ratingModel);
        if (ratingModel.getId() == null) {
            ratingModel.setId(RuleBasedModel.generateIdStr());
            log.info(String.format("Creating a rule based model with id %s for ratingEngine %s", ratingModel.getId(),
                    ratingEngineId));
            return ruleBasedModelEntityMgr.createRuleBasedModel(ratingModel, ratingEngineId);
        } else {
            RuleBasedModel retrievedRuleBasedModel = ruleBasedModelEntityMgr.findById(ratingModel.getId());
            if (retrievedRuleBasedModel == null) {
                log.warn(String.format("RuleBasedModel with id %s is not found. Creating a new one",
                        ratingModel.getId()));
                return ruleBasedModelEntityMgr.createRuleBasedModel(ratingModel, ratingEngineId);
            } else {
                findRatingModelAttributeLookups(retrievedRuleBasedModel);
                return ruleBasedModelEntityMgr.updateRuleBasedModel(ratingModel, retrievedRuleBasedModel,
                        ratingEngineId);
            }
        }
    }

    @Override
    public void deleteById(String Id) {
        ruleBasedModelEntityMgr.deleteById(Id);
    }

    @Override
    public void findRatingModelAttributeLookups(RuleBasedModel ratingModel) {
        ruleBasedModelDependencyUtil.findRatingModelAttributeLookups(ratingModel);
    }
}
