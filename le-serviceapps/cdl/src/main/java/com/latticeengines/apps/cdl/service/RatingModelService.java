package com.latticeengines.apps.cdl.service;

import java.util.List;

public interface RatingModelService<RatingModel> {

    List<RatingModel> getAllRatingModelsByRatingEngineId(String ratingEngineId);

    RatingModel geRatingModelById(String id);

    RatingModel createOrUpdate(RatingModel ratingModel, String ratingEngineId);

    void deleteById(String Id);
}
