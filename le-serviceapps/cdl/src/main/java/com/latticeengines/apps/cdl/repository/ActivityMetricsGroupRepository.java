package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ActivityMetricsGroupRepository extends BaseJpaRepository<ActivityMetricsGroup, Long> {

    ActivityMetricsGroup findByPid(@Param("pid") Long pid);

    @Query("SELECT g.groupId FROM ActivityMetricsGroup g WHERE g.groupId LIKE :base AND g.tenant = :tenant ORDER BY g.pid DESC")
    List<String> findGroupIdsByBase(@Param("tenant") Tenant tenant, @Param("base") String base);

    List<ActivityMetricsGroup> findByTenant(@Param("tenant") Tenant tenant);
}
