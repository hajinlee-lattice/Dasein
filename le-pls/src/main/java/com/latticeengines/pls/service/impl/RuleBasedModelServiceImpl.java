package com.latticeengines.pls.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.pls.entitymanager.RuleBasedModelEntityMgr;
import com.latticeengines.pls.service.RuleBasedModelService;

@Component("ruleBasedModelService")
public class RuleBasedModelServiceImpl extends RatingModelServiceBase<RuleBasedModel> implements RuleBasedModelService {

    protected RuleBasedModelServiceImpl() {
        super(RatingEngineType.RULE_BASED);
    }

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelServiceImpl.class);

    @Autowired
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Override
    public List<RuleBasedModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        return ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
    }

    @Override
    public RuleBasedModel geRatingModelById(String id) {
        return ruleBasedModelEntityMgr.findById(id);
    }

    @Override
    public RuleBasedModel createOrUpdate(RuleBasedModel ratingModel, String ratingEngineId) {
        log.info(String.format("Creating/Updating a rule based model for Rating Engine %s", ratingEngineId));
        return ruleBasedModelEntityMgr.createOrUpdateRuleBasedModel(ratingModel, ratingEngineId);
    }

    @Override
    public void deleteById(String Id) {
        ruleBasedModelEntityMgr.deleteById(Id);
    }

}
