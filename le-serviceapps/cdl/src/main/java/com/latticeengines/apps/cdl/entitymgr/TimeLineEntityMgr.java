package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.security.Tenant;

public interface TimeLineEntityMgr extends BaseEntityMgrRepository<TimeLine, Long> {

    TimeLine findByPid(Long pid);

    TimeLine findByTimeLineId(String timelineId);

    List<TimeLine> findByTenant(Tenant tenant);
}
