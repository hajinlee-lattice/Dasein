package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityMetricsService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

@Service("activityMetricsService")
public class ActivityMetricsServiceImpl implements ActivityMetricsService {
    @Inject
    private ActivityMetricsEntityMgr entityMgr;

    @Override
    public List<ActivityMetrics> findWithType(ActivityType type) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ActivityMetricsUtils.fakeMetrics(tenant);
    }

    @Override
    public List<ActivityMetrics> findActiveWithType(ActivityType type) {
        return entityMgr.findActiveWithType(type);
    }

    @Override
    public List<ActivityMetrics> save(ActivityType type, List<ActivityMetrics> metricsList) {
        metricsList.forEach(metrics -> {
            metrics.setType(type);
        });
        return entityMgr.save(metricsList);
    }
}
