package com.latticeengines.dataplatform.service;

import com.latticeengines.domain.exposed.modeling.review.ModelReviewResults;
import com.latticeengines.domain.exposed.modeling.review.RuleRemediationEnablement;

public interface ModelReviewService {

    ModelReviewResults getReviewResults(String modelId);

    RuleRemediationEnablement getRuleRemediationEnablement(String modelId);

    void setRuleRemediationEnablement(String modelId, RuleRemediationEnablement ruleChoice);

}
