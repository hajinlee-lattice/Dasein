package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelWithPublishedHistoryDTO;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public interface RatingEngineService {

    List<RatingEngine> getAllRatingEngines();

    List<RatingEngine> getAllDeletedRatingEngines();

    List<RatingEngineSummary> getAllRatingEngineSummaries();

    List<RatingEngineSummary> getAllRatingEngineSummaries(String type, String status);

    List<RatingEngineSummary> getAllRatingEngineSummaries(String type, String status,
            boolean publishedRatingsOnly);

    List<String> getAllRatingEngineIdsInSegment(String segmentName);

    List<String> getAllRatingEngineIdsInSegment(String segmentName, //
            boolean considerPublishedOnly);

    RatingEngine getRatingEngineById(String id, boolean populateRefreshedDate,
            boolean populateActiveModel);

    RatingEngine getRatingEngineById(String id, boolean populateRefreshedDate);

    RatingEngine createOrUpdate(RatingEngine ratingEngine);

    RatingEngine createOrUpdate(RatingEngine ratingEngine, Boolean unlinkSegment);

    RatingModel createModelIteration(RatingEngine ratingEngine, RatingModel ratingModel);

    RatingEngine replicateRatingEngine(String id);

    Map<String, Long> updateRatingEngineCounts(String engineId);

    void deleteById(String id);

    void deleteById(String id, boolean hardDelete, String actionInitiator);

    void revertDelete(String id);

    List<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId);

    RatingModel getRatingModel(String ratingEngineId, String ratingModelId);

    RatingModel updateRatingModel(String ratingEngineId, String ratingModelId,
            RatingModel ratingModel);

    Map<String, List<ColumnMetadata>> getIterationAttributes(String customerSpace,
            String ratingEngineId, String ratingModelId,
            List<CustomEventModelingConfig.DataStore> dataStores);

    List<ColumnMetadata> getIterationMetadata(String customerSpace, String ratingEngineId,
            String ratingModelId, List<CustomEventModelingConfig.DataStore> dataStores);

    Map<String, StatsCube> getIterationMetadataCube(String customerSpace, String ratingEngineId,
            String ratingModelId, List<CustomEventModelingConfig.DataStore> dataStores);

    TopNTree getIterationMetadataTopN(String customerSpace, String ratingEngineId,
            String ratingModelId, List<CustomEventModelingConfig.DataStore> dataStores);

    Map<String, List<String>> getRatingEngineDependencies(String customerSpace,
            String ratingEngineId);

    EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine,
            RatingModel ratingModel, ModelingQueryType modelingQueryType,
            DataCollection.Version version);

    Long getModelingQueryCount(String customerSpace, RatingEngine ratingEngine,
            RatingModel ratingModel, ModelingQueryType modelingQueryType,
            DataCollection.Version version);

    String modelRatingEngine(String tenantId, RatingEngine ratingEngine, AIModel aiModel,
            List<ColumnMetadata> userEditedAttributes, String userEmail);

    List<AttributeLookup> getDependentAttrsInAllModels(String ratingEngineId);

    List<AttributeLookup> getDependentAttrsInActiveModel(String ratingEngineId);

    List<AttributeLookup> getDependingAttrsInModel(RatingEngineType engineType, String modelId);

    List<RatingModel> getDependingRatingModels(List<String> attributes);

    List<RatingEngine> getDependingRatingEngines(List<String> attributes);

    void updateModelingJobStatus(String ratingEngineId, String aiModelId, JobStatus newStatus);

    void setScoringIteration(String customerSpace, String ratingEngineId, String ratingModelId,
            List<BucketMetadata> bucketMetadatas, String userEmail);

    boolean validateForModeling(String customerSpace, RatingEngine ratingEngine,
            AIModel ratingModel);

    List<RatingModelWithPublishedHistoryDTO> getPublishedHistory(String customerSpace,
            String ratingEngineId);

    List<AttributeLookup> getDependentAttrs();
}
