package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.ActivityMetricsActionConfiguration;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsEntityMgrTestNG extends CDLFunctionalTestNGBase {
    @Inject
    private ActivityMetricsEntityMgr entityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        ActionContext.remove();
    }

    @AfterClass(groups = "functional")
    public void cleanupActionContext() {
        ActionContext.remove();
    }

    @Test(groups = "functional")
    public void testMultiTenantFilter() {
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findWithType(ActivityType.PurchaseHistory)));

        List<ActivityMetrics> metricsList = constructMetricsList();
        List<ActivityMetrics> saved = entityMgr.save(metricsList);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saved));
        saved.forEach(metrics -> {
            Assert.assertNotNull(metrics.getPid());
            Assert.assertFalse(metrics.isEOL());
            Assert.assertNull(metrics.getDeprecated());
            Assert.assertNotNull(metrics.getTenant());
        });
        List<ActivityMetrics> active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(active));
        Assert.assertEquals(active.size(), 6);
        active.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
        });
        List<ActivityMetrics> all = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(all));
        Assert.assertEquals(active.size(), 6);
        all.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
        });
        Assert.assertNotNull(ActionContext.getAction());
        ActivityMetricsActionConfiguration config = (ActivityMetricsActionConfiguration) ActionContext.getAction()
                .getActionConfiguration();
        Assert.assertEquals(config.getNewCnt(), 6);
        Assert.assertEquals(config.getDeprecateCnt(), 0);
        Assert.assertEquals(config.getActivateCnt(), 0);

        metricsList = constructUpdatedMetricsList();
        saved = entityMgr.save(metricsList);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saved));
        Assert.assertEquals(saved.size(), 6);
        active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(active));
        Assert.assertEquals(active.size(), 6);
        all = entityMgr.findWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(all));
        Assert.assertEquals(all.size(), 10);
        Assert.assertNotNull(ActionContext.getAction());
        config = (ActivityMetricsActionConfiguration) ActionContext.getAction().getActionConfiguration();
        Assert.assertEquals(config.getNewCnt(), 4);
        Assert.assertEquals(config.getDeprecateCnt(), 4);
        Assert.assertEquals(config.getActivateCnt(), 0);

        metricsList = constructSecondUpdatedMetricsList();
        saved = entityMgr.save(metricsList);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saved));
        Assert.assertEquals(saved.size(), 1);
        active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(active));
        Assert.assertEquals(active.size(), 1);
        all = entityMgr.findWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(all));
        Assert.assertEquals(all.size(), 10);
        Assert.assertNotNull(ActionContext.getAction());
        config = (ActivityMetricsActionConfiguration) ActionContext.getAction().getActionConfiguration();
        Assert.assertEquals(config.getNewCnt(), 0);
        Assert.assertEquals(config.getDeprecateCnt(), 9);
        Assert.assertEquals(config.getActivateCnt(), 1);

        Tenant tenant = new Tenant("dummy");
        tenant.setPid(-1L);
        MultiTenantContext.setTenant(tenant);
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findActiveWithType(ActivityType.PurchaseHistory)));
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findWithType(ActivityType.PurchaseHistory)));
    }

    private List<ActivityMetrics> constructMetricsList() {
        List<ActivityMetrics> metricsList = new ArrayList<>();
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.SpendChange);
        TimeFilter filter1 = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Month.name(),
                Collections.singletonList(1));
        TimeFilter filter2 = new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Month.name(),
                Arrays.asList(1, 2));
        List<TimeFilter> filters = new ArrayList<>();
        filters.add(filter1);
        filters.add(filter2);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.TotalSpendOvertime);
        TimeFilter filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.TotalSpendOvertime);
        filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Month.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.AvgSpendOvertime);
        filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Quarter.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.AvgSpendOvertime);
        filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Year.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.ShareOfWallet);
        filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        return metricsList;
    }

    private List<ActivityMetrics> constructUpdatedMetricsList() {
        // Same period name, updated period value
        List<ActivityMetrics> metricsList = new ArrayList<>();
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.SpendChange);
        TimeFilter filter1 = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Month.name(),
                Collections.singletonList(1));
        TimeFilter filter2 = new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Month.name(),
                Arrays.asList(2, 3));
        List<TimeFilter> filters = new ArrayList<>();
        filters.add(filter1);
        filters.add(filter2);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        // Same period value, updated period name
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.TotalSpendOvertime);
        TimeFilter filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Quarter.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        // Updated period name + period value
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.TotalSpendOvertime);
        filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Year.name(),
                Collections.singletonList(2));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        // Same metrics
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.AvgSpendOvertime);
        filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Quarter.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        // Deprecate one AvgSpendOvertime

        // Same metrics
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.ShareOfWallet);
        filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        // New metrics
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.Margin);
        filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        return metricsList;
    }

    private List<ActivityMetrics> constructSecondUpdatedMetricsList() {
        List<ActivityMetrics> metricsList = new ArrayList<>();

        // Reactivate metrics
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.AvgSpendOvertime);
        TimeFilter filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Year.name(),
                Collections.singletonList(1));
        List<TimeFilter> filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        return metricsList;
    }

}
