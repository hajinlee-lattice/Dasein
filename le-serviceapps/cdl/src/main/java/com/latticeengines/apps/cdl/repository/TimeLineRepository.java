package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.security.Tenant;

public interface TimeLineRepository extends BaseJpaRepository<TimeLine, Long> {

    TimeLine findByPid(@Param("pid") Long pid);

    TimeLine findByTenantAndTimelineId(@Param("tenant") Tenant tenant, @Param("timelineId") String timelineId);

    TimeLine findByTenantAndEntity(@Param("tenant") Tenant tenant, @Param("entity") String entity);

    List<TimeLine> findByTenant(@Param("tenant") Tenant tenant);
}
