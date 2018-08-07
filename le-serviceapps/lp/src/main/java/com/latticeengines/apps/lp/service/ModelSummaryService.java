package com.latticeengines.apps.lp.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ModelSummaryService {

    ModelSummary createModelSummary(String rawModelSummary, String tenantId);

    ModelSummary createModelSummary(ModelSummary modelSummary, String tenantId);

    boolean modelIdinTenant(String modelId, String tenantId);

    void updatePredictors(String modelId, AttributeMap attrMap);

    ModelSummary getModelSummaryByModelId(String modelId);

    void deleteModelSummaryByModelId(String modelId);

    ModelSummary getModelSummaryEnrichedByDetails(String modelId);

    List<ModelSummary> getAllByTenant(Tenant tenant);

    ModelSummary getModelSummary(String modelId);

    ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly);

    List<ModelSummary> getModelSummaries(String selection);

    void updateModelSummary(String modelId, AttributeMap attrMap);

    List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame);

    void updateLastUpdateTime(String modelId);

    boolean downloadModelSummary(String tenantId);

    boolean downloadModelSummary(String tenantId, Map<String, String> modelApplicationIdToEventColumn);

    Set<String> getModelSummaryIds();

    Map<String, ModelSummary> getEventToModelSummary(Map<String, String> modelApplicationIdToEventColumn);
}
