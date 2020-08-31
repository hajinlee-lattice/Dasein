package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ActivityAlertsConfigRepository extends BaseJpaRepository<ActivityAlertsConfig, Long> {
    List<ActivityAlertsConfig> findAllByTenant(@Param("tenant") Tenant tenant);
}
