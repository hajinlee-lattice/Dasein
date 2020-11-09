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

import com.latticeengines.apps.cdl.service.DashboardService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;

public class DashboardServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DashboardServiceImplTestNG.class);

    @Inject
    private DashboardService dashboardService;

    private String name = "dashboard1";
    private String url = "www.dnb.com/<Industry>";
    private String updateName = "updateDashboard";
    private RetryTemplate retry;
    private Long pid;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "functional")
    public void testCreate() {
        Dashboard dashboard = new Dashboard();
        dashboard.setName(name);
        dashboard.setDashboardUrl(url);
        dashboard.setTenant(mainTestTenant);
        Dashboard created = dashboardService.createOrUpdate(mainCustomerSpace, dashboard);
        log.info("Dashboard is {}.", JsonUtils.serialize(created));
        log.info("pid is {}", created.getPid());
        Assert.assertNotNull(created.getPid());
        List<Dashboard> dashboardList = dashboardService.findAllByTenant(mainCustomerSpace);
        Assert.assertEquals(dashboardList.size(), 1);
        Assert.assertEquals(dashboardList.get(0).getName(), name);
        Assert.assertEquals(dashboardList.get(0).getDashboardUrl(), url);
        pid = dashboardList.get(0).getPid();
    }

    @Test(groups = "functional")
    public void testUpdate() {
        AtomicReference<Dashboard> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(dashboardService.findByPid(mainCustomerSpace, pid));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        Dashboard dashboard = createdAtom.get();
        Assert.assertNotNull(dashboard);
        Assert.assertEquals(dashboard.getName(), name);
        dashboard.setName(updateName);
        dashboardService.createOrUpdate(mainCustomerSpace, dashboard);
        retry.execute(context -> {
            createdAtom.set(dashboardService.findByName(mainCustomerSpace, updateName));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        Dashboard updateDashboard = createdAtom.get();
        Assert.assertEquals(updateDashboard.getPid(), pid);
        dashboardService.delete(mainCustomerSpace, updateDashboard);
        AtomicReference<List<Dashboard>> createdAtom1 = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom1.set(dashboardService.findAllByTenant(mainCustomerSpace));
            Assert.assertEquals(createdAtom1.get().size(), 0);
            return true;
        });
        Assert.assertEquals(createdAtom1.get().size(), 0);
    }

}
