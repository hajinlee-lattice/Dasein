package com.latticeengines.proxy.exposed.lp;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryProxy {

    void setDownloadFlag(String customerSpace);

    ModelSummary getModelSummaryByModelId(String customerSpace, String modelId);

    // List<?> getActiveModelSummaries(CustomerSpace customerSpace);
    //
    // void createModelSummary(ModelSummary modelSummary, CustomerSpace
    // customerSpace);
    //
    // void deleteModelSummary(String modelId, CustomerSpace customerSpace);
    //
    // List<ModelSummary> getPaginatedModels(CustomerSpace customerSpace, String
    // start, int offset, int maximum,
    // boolean considerAllStatus);
    //
    // int getModelsCount(CustomerSpace customerSpace, String start, boolean
    // considerAllStatus);
    //
    // List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long
    // timeFrame);

    boolean downloadModelSummary(String customerSpace);

    boolean downloadModelSummary(String customerSpace, Map<String, String> modelApplicationIdToEventColumn);

    Map<String, ModelSummary> getEventToModelSummary(String customerSpace,
            Map<String, String> modelApplicationIdToEventColumn);

    List<ModelSummary> getModelSummaries(String customerSpace, String selection);
}
