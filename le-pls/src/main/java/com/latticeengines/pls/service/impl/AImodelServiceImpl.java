package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.pls.service.AImodelService;

@Component("aiModelService")
public class AImodelServiceImpl extends RatingModelServiceBase<AIModel> implements AImodelService {

    protected AImodelServiceImpl() {
        super(RatingEngineType.AI_BASED);
    }

    @Override
    public List<AIModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AIModel geRatingModelById(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AIModel createOrUpdate(AIModel ratingModel, String ratingEngineId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteById(String Id) {
        // TODO Auto-generated method stub

    }

}
