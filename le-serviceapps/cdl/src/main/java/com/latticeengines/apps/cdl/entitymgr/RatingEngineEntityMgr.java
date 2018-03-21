package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingEngineEntityMgr {

    RatingEngine createOrUpdateRatingEngine(RatingEngine ratingEngine, String tenantId);

    List<RatingEngine> findAll();

    List<RatingEngine> findAllByTypeAndStatus(String type, String status);

    List<String> findAllIdsInSegment(String segmentName);

    RatingEngine findById(String id);

    RatingEngine findById(String id, boolean withActiveModel);

    void deleteById(String id);

    void deleteById(String id, boolean hardDelete);

    void deleteRatingEngine(RatingEngine ratingEngine);

    void deleteRatingEngine(RatingEngine ratingEngine, boolean hardDelete);

}
