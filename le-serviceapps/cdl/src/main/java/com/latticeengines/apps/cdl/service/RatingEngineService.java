package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.RatingEngineDependencyType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public interface RatingEngineService {

    List<RatingEngine> getAllRatingEngines();

    List<RatingEngine> getAllDeletedRatingEngines();

    List<RatingEngineSummary> getAllRatingEngineSummaries();

    List<RatingEngineSummary> getAllRatingEngineSummaries(String type, String status);

    List<RatingEngineSummary> getAllRatingEngineSummaries(String type, String status, boolean publishedRatingsOnly);

    List<String> getAllRatingEngineIdsInSegment(String segmentName);

    RatingEngine getRatingEngineById(String id, boolean populateRefreshedDate, boolean populateActiveModel);

    RatingEngine getRatingEngineById(String id, boolean populateRefreshedDate);

    RatingEngine createOrUpdate(RatingEngine ratingEngine);

    RatingEngine createOrUpdate(RatingEngine ratingEngine, Boolean unlinkSegment);

    RatingEngine replicateRatingEngine(String id);

    Map<String, Long> updateRatingEngineCounts(String engineId);

    void deleteById(String id);

    void deleteById(String id, boolean hardDelete);

    void revertDelete(String id);

    List<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId);

    RatingModel getRatingModel(String ratingEngineId, String ratingModelId);

    RatingModel updateRatingModel(String ratingEngineId, String ratingModelId, RatingModel ratingModel);

    Map<RatingEngineDependencyType, List<String>> getRatingEngineDependencies(String customerSpace,
            String ratingEngineId);

    EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel,
            ModelingQueryType modelingQueryType, DataCollection.Version version);

    Long getModelingQueryCount(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel,
            ModelingQueryType modelingQueryType, DataCollection.Version version);

    String modelRatingEngine(String tenantId, RatingEngine ratingEngine, AIModel aiModel, String userEmail);

    List<AttributeLookup> getDependentAttrsInAllModels(String ratingEngineId);

    List<AttributeLookup> getDependentAttrsInActiveModel(String ratingEngineId);

    List<AttributeLookup> getDependingAttrsInModel(RatingEngineType engineType, String modelId);

    List<RatingModel> getDependingRatingModels(List<String> attributes);

    List<RatingEngine> getDependingRatingEngines(List<String> attributes);

    void verifyRatingEngineCyclicDependency(RatingEngine ratingEngine);

    void updateModelingJobStatus(String ratingEngineId, String aiModelId, JobStatus newStatus);

    void setScoringIteration(String ratingEngineId, String ratingModelId, List<BucketMetadata> bucketMetadatas);
}
