package com.latticeengines.pls.service;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;

public interface RatingEngineService {

    List<RatingEngine> getAllRatingEngines();

    List<RatingEngineSummary> getAllRatingEngineSummaries();

    RatingEngine getRatingEngineById(String id);

    RatingEngine getFullRatingEngineById(String id);

    RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId);

    void deleteById(String Id);

    Set<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId);

    RatingModel getRatingModel(String ratingEngineId, String ratingModelId);

    RatingModel updateRatingModel(String ratingEngineId, String ratingModelId, RatingModel ratingModel);

}
