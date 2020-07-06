package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.security.Tenant;

public interface JourneyStageRepository extends BaseJpaRepository<JourneyStage, Long> {

    JourneyStage findByPid(@Param("pid") Long pid);

    List<JourneyStage> findByTenant(@Param("tenant") Tenant tenant);
}
