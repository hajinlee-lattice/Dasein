package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DashboardFilterService;
import com.latticeengines.apps.cdl.service.DashboardService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;

public class DashboardFilterImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DashboardFilterImplTestNG.class);

    @Inject
    private DashboardService dashboardService;
    @Inject
    private DashboardFilterService dashboardFilterService;

    private String dashboardName = "dashboard1";
    private String dashboardUrl = "www.dnb.com/<Industry>";
    private String filterName = "filter1";
    private String updateFilterName = "DateStr";
    private String filterValue = "{\"year\", \"month\", \"day\"}";
    private RetryTemplate retry;
    private Long filterPid;
    private Dashboard created;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "functional")
    public void testCreate() {
        Dashboard dashboard = new Dashboard();
        dashboard.setName(dashboardName);
        dashboard.setDashboardUrl(dashboardUrl);
        dashboard.setTenant(mainTestTenant);
        created = dashboardService.createOrUpdate(mainCustomerSpace, dashboard);
        log.info("Dashboard is {}.", JsonUtils.serialize(created));
        log.info("pid is {}", created.getPid());
        Assert.assertNotNull(created.getPid());
        List<Dashboard> dashboardList = dashboardService.findAllByTenant(mainCustomerSpace);
        Assert.assertEquals(dashboardList.size(), 1);
        Assert.assertEquals(dashboardList.get(0).getName(), dashboardName);
        Assert.assertEquals(dashboardList.get(0).getDashboardUrl(), dashboardUrl);
        DashboardFilter filter = new DashboardFilter();
        filter.setName(filterName);
        filter.setFilterValue(filterValue);
        filter.setTenant(mainTestTenant);
        filter.setDashboard(created);
        DashboardFilter createdFilter = dashboardFilterService.createOrUpdate(mainCustomerSpace, filter);
        log.info("DashboardFilter is {}.", JsonUtils.serialize(createdFilter));
        log.info("DashboardFilter pid is {}", createdFilter.getPid());
        Assert.assertNotNull(createdFilter.getPid());
        List<DashboardFilter> dashboardFilterList = dashboardFilterService.findAllByTenant(mainCustomerSpace);
        Assert.assertEquals(dashboardFilterList.size(), 1);
        Assert.assertEquals(dashboardFilterList.get(0).getName(), filterName);
        Assert.assertEquals(dashboardFilterList.get(0).getFilterValue(), filterValue);
        filterPid = dashboardFilterList.get(0).getPid();
    }

    @Test(groups = "functional")
    public void testUpdate() {
        AtomicReference<DashboardFilter> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(dashboardFilterService.findByPid(mainCustomerSpace, filterPid));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        DashboardFilter filter = createdAtom.get();
        Assert.assertNotNull(filter);
        Assert.assertEquals(filter.getName(), filterName);
        filter.setName(updateFilterName);
        dashboardFilterService.createOrUpdate(mainCustomerSpace, filter);
        retry.execute(context -> {
            createdAtom.set(dashboardFilterService.findByNameAndDashboard(mainCustomerSpace, updateFilterName,
                    created));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        DashboardFilter updateFilter = createdAtom.get();
        Assert.assertEquals(updateFilter.getPid(), filterPid);
        AtomicReference<List<DashboardFilter>> createdAtom1 = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom1.set(dashboardFilterService.findAllByDashboard(mainCustomerSpace, created));
            Assert.assertEquals(createdAtom1.get().size(), 1);
            return true;
        });
        Assert.assertEquals(createdAtom1.get().size(), 1);
        List<DashboardFilter> dashboardFilterList = createdAtom1.get();
        Assert.assertEquals(dashboardFilterList.get(0).getName(), updateFilterName);
        dashboardFilterService.delete(mainCustomerSpace, updateFilter);
        retry.execute(context -> {
            createdAtom1.set(dashboardFilterService.findAllByDashboard(mainCustomerSpace, created));
            Assert.assertEquals(createdAtom1.get().size(), 0);
            return true;
        });
        Assert.assertEquals(createdAtom1.get().size(), 0);
    }
}
