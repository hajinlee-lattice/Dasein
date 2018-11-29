package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;

public interface TalkingPointEntityMgr extends BaseEntityMgr<TalkingPoint> {
    List<TalkingPoint> findAllByPlayName(String playName);

    TalkingPoint findByName(String name);
}
