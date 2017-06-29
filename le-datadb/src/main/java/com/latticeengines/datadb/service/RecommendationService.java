package com.latticeengines.datadb.service;

import com.latticeengines.domain.exposed.datadb.Recommendation;

public interface RecommendationService {

    void create(Recommendation entity);

    Recommendation findByLaunchId(String launchId);
}
