package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DashboardEntityMgr extends BaseEntityMgrRepository<Dashboard, Long> {

    Dashboard findByPid(Long pid);

    Dashboard findByTenantAndName(Tenant tenant, String name);

    List<Dashboard> findAllByTenant(Tenant tenant);
}
