package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.DashboardFilterEntityMgr;
import com.latticeengines.apps.cdl.service.DashboardFilterService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
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
        DashboardFilter dashboardFilter1 = dashboardFilterEntityMgr.findByPid(dashboardFilter.getPid());
        if (dashboardFilter1 == null) {
            dashboardFilter1 = new DashboardFilter();
        }
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
        DashboardFilter dashboardFilter1 = dashboardFilterEntityMgr.findByPid(dashboardFilter.getPid());
        if (dashboardFilter1 == null) {
            log.error("can't find dashboardFilter item in db. input dashboardFilter is {}.", dashboardFilter);
            return;
        }
        dashboardFilterEntityMgr.delete(dashboardFilter1);
    }

    @Override
    public DashboardFilter findByPid(String customerSpace, Long pid) {
        return dashboardFilterEntityMgr.findByPid(pid);
    }

    @Override
    public DashboardFilter findByName(String customerSpace, String name) {
        return dashboardFilterEntityMgr.findByName(name);
    }

    @Override
    public List<DashboardFilter> findAllByTenant(String customerSpace) {
        return dashboardFilterEntityMgr.findAll();
    }
}
