package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.security.Tenant;

public interface JourneyStageEntityMgr extends BaseEntityMgrRepository<JourneyStage, Long> {

    JourneyStage findByPid(Long pid);

    JourneyStage findByTenantAndStageName(Tenant tenant, String stageName);

    List<JourneyStage> findByTenant(Tenant tenant);
}
