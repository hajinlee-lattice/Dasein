package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DashboardRepository extends BaseJpaRepository<Dashboard, Long> {

    Dashboard findByPid(@Param("pid") Long pid);

    Dashboard findByNameAndTenant(@Param("name") String name, @Param("tenant") Tenant tenant);

    List<Dashboard> findAllByTenant(@Param("tenant") Tenant tenant);
}
