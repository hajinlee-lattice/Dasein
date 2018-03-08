package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ActivityMetricsDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

@Component("activityMetricsDao")
public class ActivityMetricsDaoImpl extends BaseDaoImpl<ActivityMetrics> implements ActivityMetricsDao {
    @Override
    protected Class<ActivityMetrics> getEntityClass() {
        return ActivityMetrics.class;
    }
}
