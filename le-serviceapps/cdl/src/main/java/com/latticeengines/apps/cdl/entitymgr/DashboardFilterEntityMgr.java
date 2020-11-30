package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;

public interface DashboardFilterEntityMgr extends BaseEntityMgrRepository<DashboardFilter, Long> {

    DashboardFilter findByPid(Long pid);

    DashboardFilter findByName(String name);

    List<DashboardFilter> findAllByTenant();
}
