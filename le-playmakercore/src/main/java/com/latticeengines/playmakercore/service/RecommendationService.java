package com.latticeengines.playmakercore.service;

import java.util.List;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;

public interface RecommendationService {

    void create(Recommendation entity);

    List<Recommendation> findByLaunchId(String launchId);
}
