package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DashboardFilterRepository extends BaseJpaRepository<DashboardFilter, Long> {

    DashboardFilter findByPid(@Param("pid") Long pid);

    DashboardFilter findByNameAndTenant(@Param("name") String name, @Param("tenant") Tenant tenant);

    List<DashboardFilter> findAllByTenant(@Param("tenant") Tenant tenant);
}
