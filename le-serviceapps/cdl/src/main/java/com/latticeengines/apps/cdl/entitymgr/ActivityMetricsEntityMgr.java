package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public interface ActivityMetricsEntityMgr extends BaseEntityMgrRepository<ActivityMetrics, Long> {
    List<ActivityMetrics> findAll();

    List<ActivityMetrics> findAllActive();

    List<ActivityMetrics> save(List<ActivityMetrics> metricsList);
}
