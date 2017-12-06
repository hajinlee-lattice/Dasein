package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.service.impl.AIModelServiceImplDeploymentTestNG;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

public class AIModelResourceDeploymentTestNG extends AIModelServiceImplDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(AIModelResourceDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    
    @Inject
    private RatingEngineProxy ratingEngineProxy;


    @Override
	protected RatingEngine createRatingEngine(RatingEngine ratingEngine) {
		return ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
	}	

    @Override
	protected RatingModel getRatingModel() {
    		log.info("Getting Rating Model from Proxy: " + aiRatingModelId);
    		RatingModel ratingModel = ratingEngineProxy.getRatingModel(mainTestTenant.getId(), aiRatingEngineId, aiRatingModelId);
    		return ratingModel;
	}
	
    @Override
	protected void updateRatingModel(AIModel aiModel) {
    		ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), aiRatingEngineId, aiRatingModelId, aiModel);
	}
	
    @Override
	protected List<RatingEngineSummary> getAllRatingEngineSummaries() {
		return ratingEngineProxy.getRatingEngineSummaries(mainTestTenant.getId());
	}
    
    @Override
	protected RatingEngine getRatingEngineById(String ratingEngineId) {
		return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), ratingEngineId);
	}
	
    @Override
	protected void deleteRatingEngine(RatingEngine ratingEngine) {
    	    ratingEngineProxy.deleteRatingEngine(mainTestTenant.getId(), ratingEngine.getId());
	}
}
