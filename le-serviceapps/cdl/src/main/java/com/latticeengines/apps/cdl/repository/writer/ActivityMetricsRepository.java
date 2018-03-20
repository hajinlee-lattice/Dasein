package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public interface ActivityMetricsRepository extends BaseJpaRepository<ActivityMetrics, Long> {
    List<ActivityMetrics> findAllByIsEOLAndType(boolean isEOL, ActivityType type);

    List<ActivityMetrics> findAllByType(ActivityType type);

    List<ActivityMetrics> findAllByTenant(Tenant tenant);

}
