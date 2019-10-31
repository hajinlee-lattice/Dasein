package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingEngineEntityMgr extends BaseEntityMgrRepository<RatingEngine, Long> {

    List<RatingEngine> findAll();

    List<RatingEngine> findAllDeleted();

    List<String> findAllActiveModels();

    List<RatingEngine> findAllByTypeAndStatus(String type, String status);

    List<String> findAllIdsInSegment(String segmentName);

    RatingEngine findById(String id);

    RatingEngine findById(String id, boolean withActiveModel);

    void deleteById(String id, boolean hardDelete, String actionInitiator);

    void revertDelete(String id);

    RatingEngine createRatingEngine(RatingEngine ratingEngine);

    RatingEngine updateRatingEngine(RatingEngine ratingEngine, RatingEngine retrievedRatingEngine,
            Boolean unlinkSegment);

}
