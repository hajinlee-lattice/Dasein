package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;

@Component("aiModelService")
public class AIModelServiceImpl extends RatingModelServiceBase<AIModel> implements AIModelService {

	@Autowired
    private AIModelEntityMgr aiModelEntityMgr;
	
    protected AIModelServiceImpl() {
        super(RatingEngineType.AI_BASED);
    }

    @Override
    public List<AIModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        return aiModelEntityMgr.findByRatingEngineId(ratingEngineId, null);
    }

    @Override
    public AIModel geRatingModelById(String id) {
        return aiModelEntityMgr.findById(id);
    }

    @Override
    public AIModel createOrUpdate(AIModel ratingModel, String ratingEngineId) {
        aiModelEntityMgr.createOrUpdateAIModel(ratingModel, ratingEngineId);
        return ratingModel;
    }

    @Override
    public void deleteById(String id) {
    	    aiModelEntityMgr.deleteById(id);
    }

}
