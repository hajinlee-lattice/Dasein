package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.DashboardFilterEntityMgr;
import com.latticeengines.apps.cdl.service.DashboardFilterService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;

@Service("dashboardFilterService")
public class DashboardFilterServiceImpl implements DashboardFilterService {

    private static final Logger log = LoggerFactory.getLogger(DashboardFilterServiceImpl.class);

    @Inject
    private DashboardFilterEntityMgr dashboardFilterEntityMgr;

    @Override
    public DashboardFilter createOrUpdate(String customerSpace, DashboardFilter dashboardFilter) {
        if (dashboardFilter == null) {
            log.error("DashboardFilter can't be null.");
            return null;
        }
        DashboardFilter dashboardFilter1 = dashboardFilterEntityMgr.findByNameAndDashboard(dashboardFilter.getName(),
                dashboardFilter.getDashboard());
        if (dashboardFilter1 == null) {
            dashboardFilter1 = new DashboardFilter();
        } else if (dashboardFilter.getPid() == null || (dashboardFilter.getPid() != null && !dashboardFilter.getPid().equals(dashboardFilter1.getPid()))) {
            log.error("already exist an DashboardFilter, name: {}, Dashboard: {} in this tenant.",
                    dashboardFilter.getName(), dashboardFilter.getDashboard());
        }
        dashboardFilter1.setDashboard(dashboardFilter.getDashboard());
        dashboardFilter1.setFilterValue(dashboardFilter.getFilterValue());
        dashboardFilter1.setName(dashboardFilter.getName());
        dashboardFilter1.setTenant(MultiTenantContext.getTenant());
        dashboardFilterEntityMgr.createOrUpdate(dashboardFilter1);
        return dashboardFilter1;
    }

    @Override
    public void delete(String customerSpace, DashboardFilter dashboardFilter) {
        if (dashboardFilter == null) {
            log.error("DashboardFilter can't be null.");
            return;
        }
        DashboardFilter dashboardFilter1 = dashboardFilterEntityMgr.findByNameAndDashboard(dashboardFilter.getName(),
                dashboardFilter.getDashboard());
        if (dashboardFilter1 == null) {
            log.error("can't find match dashboardFilter item in db. input dashboardFilter is {}.", dashboardFilter);
            return;
        }
        if (!dashboardFilter.getPid().equals(dashboardFilter1.getPid())) {
            log.error("input item can't match the db item, input: {}, db: {}.", dashboardFilter, dashboardFilter1);
            return;
        }
        dashboardFilterEntityMgr.delete(dashboardFilter1);
    }

    @Override
    public DashboardFilter findByPid(String customerSpace, Long pid) {
        return dashboardFilterEntityMgr.findByPid(pid);
    }

    @Override
    public DashboardFilter findByNameAndDashboard(String customerSpace, String name, Dashboard dashboard) {
        return dashboardFilterEntityMgr.findByNameAndDashboard(name, dashboard);
    }

    @Override
    public List<DashboardFilter> findAllByDashboard(String customerSpace, Dashboard dashboard) {
        return dashboardFilterEntityMgr.findAllByDashboard(dashboard);
    }

    @Override
    public List<DashboardFilter> findAllByTenant(String customerSpace) {
        return dashboardFilterEntityMgr.findAll();
    }
}
