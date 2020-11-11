package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;

public interface DashboardFilterService {

    DashboardFilter createOrUpdate(String customerSpace, DashboardFilter dashboardFilter);

    void delete(String customerSpace, DashboardFilter dashboardFilter);

    DashboardFilter findByPid(String customerSpace, Long pid);

    DashboardFilter findByName(String customerSpace, String name);

    List<DashboardFilter> findAllByTenant(String customerSpace);
}
