package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public interface ActivityMetricsRepository extends BaseJpaRepository<ActivityMetrics, Long> {
    List<ActivityMetrics> findAllByIsEOL(boolean isEOL);
    
    List<ActivityMetrics> findAllByTenant(Tenant tenant);

}
