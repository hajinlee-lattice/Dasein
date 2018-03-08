package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public interface ActivityMetricsService {
    List<ActivityMetrics> findAllActive();

    List<ActivityMetrics> save(List<ActivityMetrics> metricsList);
}
