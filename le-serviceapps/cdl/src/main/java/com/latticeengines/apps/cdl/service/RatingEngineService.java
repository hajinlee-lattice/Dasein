package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;

public interface RatingEngineService {

    List<RatingEngine> getAllRatingEngines();

    List<RatingEngineSummary> getAllRatingEngineSummaries();

    List<RatingEngineSummary> getAllRatingEngineSummariesWithTypeAndStatus(String type, String status);

    List<String> getAllRatingEngineIdsInSegment(String segmentName);

    RatingEngine getRatingEngineById(String id, boolean populateRefreshedDate);

    RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId);

    Map<String, Long> updateRatingEngineCounts(String engineId);

    void deleteById(String Id);

    Set<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId);

    RatingModel getRatingModel(String ratingEngineId, String ratingModelId);

    RatingModel updateRatingModel(String ratingEngineId, String ratingModelId, RatingModel ratingModel);

}
