package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;

public interface DashboardService {

    Dashboard createOrUpdate(String customerSpace, Dashboard dashboard);

    boolean delete(String customerSpace, Dashboard dashboard);

    Dashboard findByPid(String customerSpce, Long pid);

    Dashboard findByName(String customerSpace, String name);

    List<Dashboard> findAllByTenant(String customerSpace);
}
