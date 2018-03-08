package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics.ActivityType;

public interface ActivityMetricsService {
    List<ActivityMetrics> findActiveWithType(ActivityType type);

    List<ActivityMetrics> save(ActivityType type, List<ActivityMetrics> metricsList);
}
