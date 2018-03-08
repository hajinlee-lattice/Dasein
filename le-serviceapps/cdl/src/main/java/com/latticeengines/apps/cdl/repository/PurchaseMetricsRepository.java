package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.PurchaseMetrics;

public interface PurchaseMetricsRepository extends BaseJpaRepository<PurchaseMetrics, Long> {
    List<PurchaseMetrics> findAllByIsEOL(boolean isEOL);
    
    List<PurchaseMetrics> findAllByTenant(Tenant tenant);

}
