package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ModelSummaryService {

    ModelSummary createModelSummary(String rawModelSummary, String tenantId);

    ModelSummary createModelSummary(ModelSummary modelSummary, String tenantId);

    boolean modelIdinTenant(String modelId, String tenantId);

    void updatePredictors(String modelId, AttributeMap attrMap);

    ModelSummary getModelSummaryByModelId(String modelId);

    ModelSummary getModelSummaryEnrichedByDetails(String modelId);

    List<ModelSummary> getAllByTenant(Tenant tenant);

    ModelSummary getModelSummary(String modelId);

    ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly);

    List<ModelSummary> getModelSummaries(String selection);
}
