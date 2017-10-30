package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;

public interface RatingEngineNoteEntityMgr extends BaseEntityMgr<RatingEngineNote> {

    List<RatingEngineNote> getAllByRatingEngine(RatingEngine ratingEngine);

    RatingEngineNote findById(String id);

}
