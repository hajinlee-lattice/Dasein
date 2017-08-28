package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.AImodel;
import com.latticeengines.pls.service.AImodelService;

@Component("aiModelService")
public class AImodelServiceImpl implements AImodelService {

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
