package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
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
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilterValue;

public class DashboardFilterServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DashboardFilterServiceImplTestNG.class);

    @Inject
    private DashboardFilterService dashboardFilterService;

    private String filterName = "filter1";
    private String updateFilterName = "DateStr";
    private List<DashboardFilterValue> filterValue;
    private RetryTemplate retry;
    private Long filterPid;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        filterValue = createDashboardFilterValue();
    }

    @Test(groups = "functional")
    public void testCreate() {
        DashboardFilter filter = new DashboardFilter();
        filter.setName(filterName);
        filter.setFilterValue(filterValue);
        filter.setTenant(mainTestTenant);
        DashboardFilter createdFilter = dashboardFilterService.createOrUpdate(mainCustomerSpace, filter);
        log.info("DashboardFilter is {}.", JsonUtils.serialize(createdFilter));
        log.info("DashboardFilter pid is {}", createdFilter.getPid());
        Assert.assertNotNull(createdFilter.getPid());
        List<DashboardFilter> dashboardFilterList = dashboardFilterService.findAllByTenant(mainCustomerSpace);
        log.info("dashboard list is {}.", JsonUtils.serialize(dashboardFilterList));
        Assert.assertEquals(dashboardFilterList.size(), 1);
        Assert.assertEquals(dashboardFilterList.get(0).getName(), filterName);
        Assert.assertEquals(dashboardFilterList.get(0).getFilterValue().get(0).getDisplayName(),
                filterValue.get(0).getDisplayName());
        filterPid = dashboardFilterList.get(0).getPid();
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testUpdate() {
        AtomicReference<DashboardFilter> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(dashboardFilterService.findByPid(mainCustomerSpace, filterPid));
            DashboardFilter filter = createdAtom.get();
            Assert.assertNotNull(filter);
            Assert.assertEquals(filter.getName(), filterName);
            return true;
        });
        DashboardFilter filter = createdAtom.get();
        filter.setName(updateFilterName);
        dashboardFilterService.createOrUpdate(mainCustomerSpace, filter);
        retry.execute(context -> {
            createdAtom.set(dashboardFilterService.findByName(mainCustomerSpace, updateFilterName));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        DashboardFilter updateFilter = createdAtom.get();
        Assert.assertEquals(updateFilter.getPid(), filterPid);
        AtomicReference<List<DashboardFilter>> createdAtom1 = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom1.set(dashboardFilterService.findAllByTenant(mainCustomerSpace));
            Assert.assertEquals(createdAtom1.get().size(), 1);
            List<DashboardFilter> dashboardFilterList = createdAtom1.get();
            Assert.assertEquals(dashboardFilterList.get(0).getName(), updateFilterName);
            return true;
        });
        dashboardFilterService.delete(mainCustomerSpace, updateFilter);
        retry.execute(context -> {
            createdAtom1.set(dashboardFilterService.findAllByTenant(mainCustomerSpace));
            Assert.assertEquals(createdAtom1.get().size(), 0);
            return true;
        });
        Assert.assertEquals(createdAtom1.get().size(), 0);
    }

    private List<DashboardFilterValue> createDashboardFilterValue() {
        List<DashboardFilterValue> values = new ArrayList<>();
        values.add(createValue("15 month", "15m"));
        values.add(createValue("last one week", "1w"));
        values.add(createValue("last two week", "2w"));
        return values;
    }
    private DashboardFilterValue createValue(String displayName, String value) {
        DashboardFilterValue filterValue = new DashboardFilterValue();
        filterValue.setDisplayName(displayName);
        filterValue.setValue(value);
        return filterValue;
    }
}
