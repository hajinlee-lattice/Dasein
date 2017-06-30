package com.latticeengines.datadb.service;

import java.util.List;

import com.latticeengines.domain.exposed.datadb.Recommendation;

public interface RecommendationService {

    void create(Recommendation entity);

    List<Recommendation> findByLaunchId(String launchId);
}
