package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics.ActivityType;

public interface ActivityMetricsEntityMgr extends BaseEntityMgrRepository<ActivityMetrics, Long> {
    List<ActivityMetrics> findWithType(ActivityType type);

    List<ActivityMetrics> findActiveWithType(ActivityType type);

    List<ActivityMetrics> save(List<ActivityMetrics> metricsList);
}
