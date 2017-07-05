package com.latticeengines.datadb.playmaker.service;

import java.util.List;

import com.latticeengines.domain.exposed.datadb.playmaker.Recommendation;

public interface RecommendationService {

    void create(Recommendation entity);

    List<Recommendation> findByLaunchId(String launchId);
}
