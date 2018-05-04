package com.latticeengines.playmakercore.service;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;

public interface RecommendationService {

    void create(Recommendation entity);

    void delete(Recommendation entity);

    List<Recommendation> findByLaunchId(String launchId);

    List<Recommendation> findRecommendations(Date lastModificationDate, //
            int offset, int max, String syncDestination, List<String> playIds, Map<String, String> orgInfo);

    int findRecommendationCount(Date lastModificationDate, //
            String syncDestination, List<String> playIds, Map<String, String> orgInfo);

    List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, //
            int offset, int max, String syncDestination, List<String> playIds, Map<String, String> orgInfo);
}
