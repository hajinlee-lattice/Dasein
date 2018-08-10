package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingModelService<RatingModel> {

    List<RatingModel> getAllRatingModelsByRatingEngineId(String ratingEngineId);

    RatingModel getRatingModelById(String id);

    RatingModel createOrUpdate(RatingModel ratingModel, String ratingEngineId);

    RatingModel createNewIteration(RatingModel ratingModel, RatingEngine ratingEngine);

    void deleteById(String Id);

    void findRatingModelAttributeLookups(RatingModel ratingModel);
}
