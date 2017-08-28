package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.pls.RuleBasedModel;

public interface RuleBasedModelEntityMgr {

    RuleBasedModel createOrUpdateRuleBasedModel(RuleBasedModel ruleBasedModel, String ratingEngineId);

    List<RuleBasedModel> findAllByRatingEngineId(String ratingEngineId);

    RuleBasedModel findById(String id);

    void deleteById(String id);

    void deleteRuleBasedModel(RuleBasedModel ruleBasedModel);
}
