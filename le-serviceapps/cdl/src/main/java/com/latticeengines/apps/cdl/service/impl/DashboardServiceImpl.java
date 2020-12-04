package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.DashboardEntityMgr;
import com.latticeengines.apps.cdl.service.DashboardService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.security.Tenant;

@Service("dashboardService")
public class DashboardServiceImpl implements DashboardService {

    private static final Logger log = LoggerFactory.getLogger(DashboardServiceImpl.class);

    @Inject
    private DashboardEntityMgr dashboardEntityMgr;

    @Override
    public Dashboard createOrUpdate(String customerSpace, Dashboard dashboard) {
        if (dashboard == null) {
            log.error("dashboard can't be null.");
            return null;
        }
        if (dashboard.getName() == null) {
            log.error("dashboardName can't be null.");
            return null;
        }
        Tenant tenant = MultiTenantContext.getTenant();
        Dashboard dashboard1 = dashboardEntityMgr.findByPid(dashboard.getPid());
        if (dashboard1 == null) {
            dashboard1 = new Dashboard();
        }
        dashboard1.setName(dashboard.getName());
        dashboard1.setDashboardUrl(dashboard.getDashboardUrl());
        dashboard1.setTenant(tenant);
        dashboardEntityMgr.createOrUpdate(dashboard1);
        return dashboard1;

    }

    @Override
    public void createOrUpdateAll(String customerSpace, List<Dashboard> dashboards) {
        if (CollectionUtils.isEmpty(dashboards)) {
            return;
        }
        for (Dashboard dashboard : dashboards) {
            createOrUpdate(customerSpace, dashboard);
        }
    }

    @Override
    public boolean delete(String customerSpace, Dashboard dashboard) {
        if (dashboard == null) {
            log.error("dashboard can't be null.");
            return false;
        }
        if (dashboard.getPid() == null) {
            log.error("dashboard pid can't be null.");
            return false;
        }
        Tenant tenant = MultiTenantContext.getTenant();
        Dashboard dashboard1 = dashboardEntityMgr.findByPid(dashboard.getPid());
        if (dashboard1 == null) {
            return false;
        }
        dashboardEntityMgr.delete(dashboard1);
        return true;
    }

    @Override
    public Dashboard findByPid(String customerSpace, Long pid) {
        return dashboardEntityMgr.findByPid(pid);
    }

    @Override
    public Dashboard findByName(String customerSpace, String name) {
        return dashboardEntityMgr.findByTenantAndName(MultiTenantContext.getTenant(), name);
    }

    @Override
    public List<Dashboard> findAllByTenant(String customerSpace) {
        return dashboardEntityMgr.findAllByTenant(MultiTenantContext.getTenant());
    }
}
