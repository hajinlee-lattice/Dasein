package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public interface ActivityMetricsService {
    List<ActivityMetrics> findActiveWithType(ActivityType type);

    List<ActivityMetrics> save(ActivityType type, List<ActivityMetrics> metricsList);
}
