package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.PublishedTalkingPoint;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;

public interface PublishedTalkingPointEntityMgr extends BaseEntityMgrRepository<PublishedTalkingPoint, Long> {
    PublishedTalkingPoint findByName(String name);

    List<PublishedTalkingPoint> findAllByPlayName(String playName);

    List<TalkingPointDTO> findAllByTenantPid(Long tenantPid);
}
