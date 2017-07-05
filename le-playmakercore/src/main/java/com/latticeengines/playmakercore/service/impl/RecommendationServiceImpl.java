package com.latticeengines.playmakercore.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;

@Component("recommendationService")
public class RecommendationServiceImpl implements RecommendationService {
    @Autowired
    private RecommendationEntityMgr recommendationEntityMgr;

    @Override
    public void create(Recommendation entity) {
        recommendationEntityMgr.create(entity);
    }

    @Override
    public List<Recommendation> findByLaunchId(String launchId) {
        return recommendationEntityMgr.findByLaunchId(launchId);
    }
}
