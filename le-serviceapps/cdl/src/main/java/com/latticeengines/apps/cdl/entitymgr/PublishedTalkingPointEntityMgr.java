package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.cdl.PublishedTalkingPoint;

public interface PublishedTalkingPointEntityMgr extends BaseEntityMgr<PublishedTalkingPoint> {
    PublishedTalkingPoint findByName(String name);

    List<PublishedTalkingPoint> findAllByPlayName(String playName);
}
