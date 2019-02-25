package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;

public interface TalkingPointEntityMgr extends BaseEntityMgrRepository<TalkingPoint, Long> {
    List<TalkingPoint> findAllByPlayName(String playName);

    TalkingPoint findByName(String name);
}
