package com.latticeengines.pls.service;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;

public interface RatingEngineService {

    List<RatingEngine> getAllRatingEngines();

    RatingEngine getRatingEngineById(String id);

    Set<RatingModel> getAllRatingModelsById(String id);

    Set<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId);

    RatingModel getRatingModel(String ratingEngineId, String ratingModelId);

    RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId);

    void deleteById(String Id);

}
