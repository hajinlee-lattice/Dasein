package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityMetricsService;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

@Service("activityMetricsService")
public class ActivityMetricsServiceImpl implements ActivityMetricsService {
    @Inject
    private ActivityMetricsEntityMgr entityMgr;

    @Override
    public List<ActivityMetrics> findAllActive() {
        return entityMgr.findAllActive();
    }

    @Override
    public List<ActivityMetrics> save(List<ActivityMetrics> metricsList) {
        return entityMgr.save(metricsList);
    }
}
