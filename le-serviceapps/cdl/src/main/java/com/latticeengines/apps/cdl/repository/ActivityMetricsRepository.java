package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics.ActivityType;

public interface ActivityMetricsRepository extends BaseJpaRepository<ActivityMetrics, Long> {
    List<ActivityMetrics> findAllByIsEOLAndType(boolean isEOL, ActivityType type);

    List<ActivityMetrics> findAllByType(ActivityType type);

    List<ActivityMetrics> findAllByTenant(Tenant tenant);

}
