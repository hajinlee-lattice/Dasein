package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;

public interface RuleBasedModelEntityMgr extends BaseEntityMgr<RuleBasedModel> {

    List<RuleBasedModel> findAllByRatingEngineId(String ratingEngineId);

    RuleBasedModel findById(String id);

    void deleteById(String id);

    void deleteRuleBasedModel(RuleBasedModel ruleBasedModel);

    MetadataSegment inflateParentSegment(RuleBasedModel ruleBasedModel);

    RuleBasedModel createRuleBasedModel(RuleBasedModel ruleBasedModel, String ratingEngineId);

    RuleBasedModel updateRuleBasedModel(RuleBasedModel ruleBasedModel, RuleBasedModel retrievedRuleBasedModel,
            String ratingEngineId);
}
