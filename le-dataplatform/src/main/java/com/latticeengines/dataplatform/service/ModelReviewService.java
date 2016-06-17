package com.latticeengines.dataplatform.service;

import com.latticeengines.domain.exposed.modeling.review.DataRuleConfiguration;
import com.latticeengines.domain.exposed.modeling.review.ModelReviewResults;

public interface ModelReviewService {

    ModelReviewResults getReviewResults(String modelId);

    DataRuleConfiguration getDataRuleConfiguration(String modelId);

    void setDataRuleConfiguration(String modelId, DataRuleConfiguration ruleConfig);

}
