package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.pls.service.RatingModelService;

@Component("ratingModelService")
public abstract class RatingModelServiceBase<RatingModel> implements RatingModelService<RatingModel> {

    @SuppressWarnings("rawtypes")
    private static Map<RatingEngineType, RatingModelService> registry = new HashMap<>();

    protected RatingModelServiceBase(RatingEngineType ratingEngineType) {
        registry.put(ratingEngineType, this);
    }

    @SuppressWarnings("rawtypes")
    public static RatingModelService getRatingModelService(RatingEngineType ratingEngineType) {
        return registry.get(ratingEngineType);
    }

}
