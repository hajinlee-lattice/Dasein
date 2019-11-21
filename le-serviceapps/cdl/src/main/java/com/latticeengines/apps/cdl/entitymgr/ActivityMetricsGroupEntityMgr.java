package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ActivityMetricsGroupEntityMgr extends BaseEntityMgrRepository<ActivityMetricsGroup, Long> {

    ActivityMetricsGroup findByPid(Long pid);

    List<ActivityMetricsGroup> findByTenant(Tenant tenant);

    List<ActivityMetricsGroup> findByStream(AtlasStream stream);

    String getNextAvailableGroupId(String base);
}
