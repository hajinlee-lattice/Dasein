package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingEngineEntityMgr {

    RatingEngine createOrUpdateRatingEngine(RatingEngine ratingEngine, String tenantId);

    List<RatingEngine> findAll();

    RatingEngine findById(String id);

    void deleteById(String id);

    void deleteRatingEngine(RatingEngine ratingEngine);

}
