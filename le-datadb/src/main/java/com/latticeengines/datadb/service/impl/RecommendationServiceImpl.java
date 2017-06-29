package com.latticeengines.datadb.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datadb.entitymanager.RecommendationEntityMgr;
import com.latticeengines.datadb.service.RecommendationService;
import com.latticeengines.domain.exposed.datadb.Recommendation;

@Component("recommendationService")
public class RecommendationServiceImpl implements RecommendationService {
    @Autowired
    private RecommendationEntityMgr recommendationEntityMgr;

    @Override
    public void create(Recommendation entity) {
        recommendationEntityMgr.create(entity);
    }

    @Override
    public Recommendation findByLaunchId(String launchId) {
        return recommendationEntityMgr.findByLaunchId(launchId);
    }
}
