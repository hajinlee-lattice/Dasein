package com.latticeengines.playmakercore.service.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.playmakercore.service.RecommendationService;

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

    @Override
    public List<Recommendation> findRecommendations(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds) {
        return recommendationEntityMgr.findRecommendations(lastModificationDate, offset, max, syncDestination, playIds);
    }

    @Override
    public int findRecommendationCount(Date lastModificationDate, String syncDestination, List<String> playIds) {
        return recommendationEntityMgr.findRecommendationCount(lastModificationDate, syncDestination, playIds);
    }

    @Override
    public List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds) {
        return recommendationEntityMgr.findRecommendationsAsMap(lastModificationDate, offset, max, syncDestination,
                playIds);
    }
}
