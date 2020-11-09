package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;

public interface DashboardFilterEntityMgr extends BaseEntityMgrRepository<DashboardFilter, Long> {

    DashboardFilter findByPid(Long pid);

    DashboardFilter findByNameAndDashboard(String name, Dashboard dashboard);

    List<DashboardFilter> findAllByDashboard(Dashboard dashboard);

    List<DashboardFilter> findAllByTenant();
}
