package com.latticeengines.proxy.exposed.lp;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.Predictor;

public interface ModelSummaryProxy {

    void setDownloadFlag(String customerSpace);

    boolean downloadModelSummary(String customerSpace);

    boolean downloadModelSummary(String customerSpace, Map<String, String> modelApplicationIdToEventColumn);

    Map<String, ModelSummary> getEventToModelSummary(String customerSpace,
            Map<String, String> modelApplicationIdToEventColumn);

    void create(String customerSpace, ModelSummary modelSummary);

    ModelSummary createModelSummary(String customerSpace, ModelSummary modelSummary, boolean usingRaw);

    boolean update(String customerSpace, String modelId, AttributeMap attrMap);

    boolean updateStatusByModelId(String customerSpace, String modelId, ModelSummaryStatus status);

    boolean deleteByModelId(String customerSpace, String modelId);

    ModelSummary getModelSummary(String customerSpace, String modelId);

    ModelSummary findByModelId(String customerSpace, String modelId,
           boolean returnRelational, boolean returnDocument, boolean validOnly);

    ModelSummary findValidByModelId(String customerSpace, String modelId);

    ModelSummary getModelSummaryEnrichedByDetails(String customerSpace, String modelId);

    ModelSummary getModelSummaryFromModelId(String customerSpace, String modelId);

    ModelSummary findByApplicationId(String customerSpace, String applicationId);

    List<ModelSummary> getModelSummaries(String customerSpace, String selection);

    List<ModelSummary> findAll(String customerSpace);

    List<ModelSummary> findAllValid(String customerSpace);

    List<?> findAllActive(String customerSpace);

    List<ModelSummary> findPaginatedModels(String customerSpace,
        String start, boolean considerAllStatus, int offset, int maximum);

    int findTotalCount(String customerSpace, String start, boolean considerAllStatus);

    boolean modelIdinTenant(String customerSpace, String modelId);

    List<Predictor> getAllPredictors(String customerSpace, String modelId);

    List<Predictor> getPredictorsForBuyerInsights(String customerSpace, String modelId);

    boolean updatePredictors(String customerSpace, String modelId, AttributeMap attrMap);

    ModelSummary getByModelId(String modelId);

    ModelSummary retrieveByModelIdForInternalOperations(String modelId);

    List<ModelSummary> getAllForTenant(String tenantName);

    List<ModelSummary> getModelSummariesByApplicationId(String applicationId);

    List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame);

    void deleteById(String id);

    void updateById(String id, NoteParams noteParams);

    void create(String modelSummaryId, NoteParams noteParams);

    List<ModelNote> getAllByModelSummaryId(String customerSpace, String modelId, boolean returnRelational,
                                           boolean returnDocument, boolean validOnly);

    void copyNotes(String sourceModelSummaryId, String targetModelSummaryId);
}
