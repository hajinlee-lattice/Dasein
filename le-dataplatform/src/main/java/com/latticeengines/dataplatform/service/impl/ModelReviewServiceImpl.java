package com.latticeengines.dataplatform.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.ModelReviewService;
import com.latticeengines.domain.exposed.modeling.review.ModelReviewResults;
import com.latticeengines.domain.exposed.modeling.review.RuleRemediationEnablement;

@Component("modelReviewService")
public class ModelReviewServiceImpl implements ModelReviewService {

    @Override
    public ModelReviewResults getReviewResults(String modelId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RuleRemediationEnablement getRuleRemediationEnablement(String modelId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setRuleRemediationEnablement(String modelId, RuleRemediationEnablement ruleChoice) {
        // TODO Auto-generated method stub

    }

}
