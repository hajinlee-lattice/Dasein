package com.latticeengines.dante.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.TalkingPoint;

public interface TalkingPointEntityMgr extends BaseEntityMgr<TalkingPoint> {
    List<TalkingPoint> findAllByPlayID(Long playExternalID);
}
