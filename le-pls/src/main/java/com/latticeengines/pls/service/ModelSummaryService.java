package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryService {

    ModelSummary createModelSummary(String rawModelSummary, String tenantId);

    ModelSummary createModelSummary(ModelSummary modelSummary, String tenantId);

    boolean modelIdinTenant(String modelId, String tenantId);

    void updatePredictors(String modelId, AttributeMap attrMap);

    ModelSummary getModelSummaryByModelId(String modelId);

    ModelSummary getModelSummaryEnrichedByDetails(String modelId);

}
