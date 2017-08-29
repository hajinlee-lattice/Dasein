package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingEngineEntityMgr {

    RatingEngine createOrUpdateRatingEngine(RatingEngine ratingEngine, String tenantId);

    List<RatingEngine> findAll();

    RatingEngine findById(String id);

    RatingEngine findById(String id, boolean inflateSegment, boolean inflateRatingModels, boolean inflatePlays);

    void deleteById(String id);

    void deleteRatingEngine(RatingEngine ratingEngine);

}
