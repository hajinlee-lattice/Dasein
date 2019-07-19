package com.latticeengines.apps.lp.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ModelSummaryService {

    void create(ModelSummary summary);

    ModelSummary getModelSummaryByModelId(String modelId);

    ModelSummary findValidByModelId(String modelId);

    ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly);

    ModelSummary findByApplicationId(String applicationId);

    List<ModelSummary> getModelSummariesByApplicationId(String applicationId);

    void deleteByModelId(String modelId);

    void delete(ModelSummary modelSummary);

    List<ModelSummary> getAll();

    List<String> getAllModelSummaryIds();

    List<ModelSummary> getAllByTenant(Tenant tenant);

    List<ModelSummary> findAllValid();

    List<ModelSummary> findAllActive();

    int findTotalCount(long lastUpdateTime, boolean considerAllStatus);

    List<ModelSummary> findPaginatedModels(long lastUpdateTime, boolean considerAllStatus, int offset, int maximum);

    void updateStatusByModelId(String modelId, ModelSummaryStatus status);

    void updateModelSummary(ModelSummary modelSummary, AttributeMap attrMap);

    ModelSummary retrieveByModelIdForInternalOperations(String modelId);

    void updatePredictors(List<Predictor> predictors, AttributeMap attrMap);

    List<Predictor> findAllPredictorsByModelId(String modelId);

    List<Predictor> findPredictorsUsedByBuyerInsightsByModelId(String modelId);

    List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame);

    void updateLastUpdateTime(String modelId);

    boolean hasBucketMetadata(String modelId);

    ModelSummary createModelSummary(String rawModelSummary, String tenantId);

    ModelSummary createModelSummary(ModelSummary modelSummary, String tenantId);

    boolean modelIdinTenant(String modelId, String tenantId);

    void updatePredictors(String modelId, AttributeMap attrMap);

    ModelSummary getModelSummaryEnrichedByDetails(String modelId);

    ModelSummary getModelSummary(String modelId);

    List<ModelSummary> getModelSummaries(String selection);

    void updateModelSummary(String modelId, AttributeMap attrMap);

    boolean downloadModelSummary(String tenantId, Map<String, String> modelApplicationIdToEventColumn);

    Set<String> getModelSummaryIds();

    Map<String, ModelSummary> getEventToModelSummary(String tenantId, Map<String, String> modelApplicationIdToEventColumn);

    List<ModelSummary> findModelSummariesByIds(Set<String> ids);
}
