package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ActivityAlertsConfigEntityMgr extends BaseEntityMgrRepository<ActivityAlertsConfig, Long> {
    List<ActivityAlertsConfig> findAllByTenant(Tenant tenant);

    ActivityAlertsConfig findByPid(Long pid);
}
