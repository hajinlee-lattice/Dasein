package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.AImodelService;
import com.latticeengines.domain.exposed.pls.AImodel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;

@Component("aiModelService")
public class AImodelServiceImpl extends RatingModelServiceBase<AImodel> implements AImodelService {

    protected AImodelServiceImpl() {
        super(RatingEngineType.AI_BASED);
    }

    @Override
    public List<AImodel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AImodel geRatingModelById(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AImodel createOrUpdate(AImodel ratingModel, String ratingEngineId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteById(String Id) {
        // TODO Auto-generated method stub

    }

}
