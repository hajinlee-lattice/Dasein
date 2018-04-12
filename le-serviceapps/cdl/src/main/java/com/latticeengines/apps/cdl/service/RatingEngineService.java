package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.RatingEngineDependencyType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public interface RatingEngineService {

    List<RatingEngine> getAllRatingEngines();

    List<RatingEngine> getAllDeletedRatingEngines();

    List<RatingEngineSummary> getAllRatingEngineSummaries();

    List<RatingEngineSummary> getAllRatingEngineSummariesWithTypeAndStatus(String type, String status);

    List<String> getAllRatingEngineIdsInSegment(String segmentName);

    RatingEngine getRatingEngineById(String id, boolean populateRefreshedDate, boolean populateActiveModel);

    RatingEngine getRatingEngineById(String id, boolean populateRefreshedDate);

    RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId);

    RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId, Boolean unlinkSegment);

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
            ModelingQueryType modelingQueryType);

    Long getModelingQueryCount(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel,
            ModelingQueryType modelingQueryType);

    String modelRatingEngine(String tenantId, RatingEngine ratingEngine, AIModel aiModel, String userEmail);

    List<RatingEngineSummary> getAllRatingEngineSummariesWithTypeAndStatusInRedShift(String type, String status,
            Boolean onlyInRedshift);

    List<AttributeLookup> getDependentAttrsInAllModels(String ratingEngineId);

    List<AttributeLookup> getDependentAttrsInActiveModel(String ratingEngineId);

    List<RatingModel> getDependingRatingModels(List<String> attributes);

    List<RatingEngine> getDependingRatingEngines(List<String> attributes);
}
