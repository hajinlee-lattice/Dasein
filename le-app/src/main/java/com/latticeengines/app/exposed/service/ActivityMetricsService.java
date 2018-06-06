package com.latticeengines.app.exposed.service;

import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetricsValidation;

public interface ActivityMetricsService {
    ActivityMetricsValidation validateActivityMetrics(String customerSpace);
}
