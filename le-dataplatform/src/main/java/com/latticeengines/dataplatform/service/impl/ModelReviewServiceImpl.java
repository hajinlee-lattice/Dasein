package com.latticeengines.dataplatform.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.ModelReviewService;
import com.latticeengines.domain.exposed.modeling.review.DataRuleConfiguration;
import com.latticeengines.domain.exposed.modeling.review.ModelReviewResults;

@Component("modelReviewService")
public class ModelReviewServiceImpl implements ModelReviewService {

    @Override
    public ModelReviewResults getReviewResults(String modelId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataRuleConfiguration getDataRuleConfiguration(String modelId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setDataRuleConfiguration(String modelId, DataRuleConfiguration ruleConfig) {
        // TODO Auto-generated method stub

    }



}
