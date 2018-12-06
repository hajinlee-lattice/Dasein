package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsEntityMgrTestNG.class);

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

        // 1-3th saves for total sp and avg sp all use WITHIN relation
        // as old behavior, they should be converted to BETWEEN when we get
        // active metrics for metrics configuration UI, but should remain as
        // original WITHIN relation when
        // getting all metrics for PA

        // ******** 1st SAVE ******** //
        List<ActivityMetrics> metricsList = constructMetricsList1();
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
            if (item.getMetrics() == InterfaceName.TotalSpendOvertime
                    || item.getMetrics() == InterfaceName.AvgSpendOvertime) {
                verifyTotalAvgSPPeriodConfig(item, ComparisonType.BETWEEN, true);
            }
        });
        List<ActivityMetrics> all = entityMgr.findWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(all));
        Assert.assertEquals(active.size(), 6);
        all.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
            if (item.getMetrics() == InterfaceName.TotalSpendOvertime
                    || item.getMetrics() == InterfaceName.AvgSpendOvertime) {
                verifyTotalAvgSPPeriodConfig(item, ComparisonType.WITHIN, false);
            }
        });
        Assert.assertNotNull(ActionContext.getAction());
        ActivityMetricsActionConfiguration config = (ActivityMetricsActionConfiguration) ActionContext.getAction()
                .getActionConfiguration();
        log.info("ActionConfiguration: " + config.serialize());
        Assert.assertEquals(config.getActivated().size(), 4);
        Assert.assertEquals(config.getUpdated().size(), 0);
        Assert.assertEquals(config.getDeactivated().size(), 0);

        // ******** 2nd SAVE ******** //
        metricsList = constructMetricsList2();
        saved = entityMgr.save(metricsList);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saved));
        Assert.assertEquals(saved.size(), 6);
        active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(active));
        Assert.assertEquals(active.size(), 6);
        active.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
            if (item.getMetrics() == InterfaceName.TotalSpendOvertime
                    || item.getMetrics() == InterfaceName.AvgSpendOvertime) {
                verifyTotalAvgSPPeriodConfig(item, ComparisonType.BETWEEN, true);
            }
        });
        all = entityMgr.findWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(all));
        Assert.assertEquals(all.size(), 10);
        all.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
            if (item.getMetrics() == InterfaceName.TotalSpendOvertime
                    || item.getMetrics() == InterfaceName.AvgSpendOvertime) {
                verifyTotalAvgSPPeriodConfig(item, ComparisonType.WITHIN, false);
            }
        });
        Assert.assertNotNull(ActionContext.getAction());
        config = (ActivityMetricsActionConfiguration) ActionContext.getAction().getActionConfiguration();
        log.info("ActionConfiguration: " + config.serialize());
        Assert.assertEquals(config.getActivated().size(), 1);
        Assert.assertEquals(config.getUpdated().size(), 4);
        Assert.assertEquals(config.getDeactivated().size(), 0);

        // ******** 3rd SAVE ******** //
        metricsList = constructMetricsList3();
        saved = entityMgr.save(metricsList);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saved));
        Assert.assertEquals(saved.size(), 1);
        active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(active));
        Assert.assertEquals(active.size(), 1);
        active.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
            if (item.getMetrics() == InterfaceName.TotalSpendOvertime
                    || item.getMetrics() == InterfaceName.AvgSpendOvertime) {
                verifyTotalAvgSPPeriodConfig(item, ComparisonType.BETWEEN, true);
            }
        });
        all = entityMgr.findWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(all));
        Assert.assertEquals(all.size(), 10);
        all.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
            if (item.getMetrics() == InterfaceName.TotalSpendOvertime
                    || item.getMetrics() == InterfaceName.AvgSpendOvertime) {
                verifyTotalAvgSPPeriodConfig(item, ComparisonType.WITHIN, false);
            }
        });
        Assert.assertNotNull(ActionContext.getAction());
        config = (ActivityMetricsActionConfiguration) ActionContext.getAction().getActionConfiguration();
        log.info("ActionConfiguration: " + config.serialize());
        Assert.assertEquals(config.getActivated().size(), 0);
        Assert.assertEquals(config.getUpdated().size(), 1);
        Assert.assertEquals(config.getDeactivated().size(), 4);

        // ******** 4th SAVE ******** //
        // Get current active metrics out and save back directly
        // Total/Avg SP should deprecate old metrics with WITHIN relation, and
        // create new metrics with BETWEEN relation automatically
        active.forEach(metrics -> {
            metrics.setPid(null);
        });
        saved = entityMgr.save(active);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saved));
        Assert.assertEquals(saved.size(), 1);
        active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(active));
        Assert.assertEquals(active.size(), 1);
        active.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
            if (item.getMetrics() == InterfaceName.TotalSpendOvertime
                    || item.getMetrics() == InterfaceName.AvgSpendOvertime) {
                verifyTotalAvgSPPeriodConfig(item, ComparisonType.BETWEEN, true);
            }
        });
        all = entityMgr.findWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(all));
        Assert.assertEquals(all.size(), 11);
        all.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
            if (item.getMetrics() == InterfaceName.TotalSpendOvertime
                    || item.getMetrics() == InterfaceName.AvgSpendOvertime) {
                if (item.isEOL()) {
                    verifyTotalAvgSPPeriodConfig(item, ComparisonType.WITHIN, false);
                } else {
                    verifyTotalAvgSPPeriodConfig(item, ComparisonType.BETWEEN, true);
                }
            }
        });
        Assert.assertNotNull(ActionContext.getAction());
        config = (ActivityMetricsActionConfiguration) ActionContext.getAction().getActionConfiguration();
        log.info("ActionConfiguration: " + config.serialize());
        Assert.assertEquals(config.getActivated().size(), 0);
        Assert.assertEquals(config.getUpdated().size(), 1);
        Assert.assertEquals(config.getDeactivated().size(), 4);

        // ******** 5th SAVE ******** //
        // Use new BETWEEN relation to add/update Total/Avg SP metrics
        metricsList = constructMetricsList5();
        saved = entityMgr.save(metricsList);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saved));
        Assert.assertEquals(saved.size(), 2);
        active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(active));
        Assert.assertEquals(active.size(), 2);
        all = entityMgr.findWithType(ActivityType.PurchaseHistory);
        Assert.assertTrue(CollectionUtils.isNotEmpty(all));
        Assert.assertEquals(all.size(), 13);
        Assert.assertNotNull(ActionContext.getAction());
        config = (ActivityMetricsActionConfiguration) ActionContext.getAction().getActionConfiguration();
        log.info("ActionConfiguration: " + config.serialize());
        Assert.assertEquals(config.getActivated().size(), 0);
        Assert.assertEquals(config.getUpdated().size(), 2);
        Assert.assertEquals(config.getDeactivated().size(), 3);

        Tenant tenant = new Tenant("dummy");
        tenant.setPid(-1L);
        MultiTenantContext.setTenant(tenant);
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findActiveWithType(ActivityType.PurchaseHistory)));
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findWithType(ActivityType.PurchaseHistory)));
    }

    private List<ActivityMetrics> constructMetricsList1() {
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
                Collections.singletonList(3));
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

    private List<ActivityMetrics> constructMetricsList2() {
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

    private List<ActivityMetrics> constructMetricsList3() {
        List<ActivityMetrics> metricsList = new ArrayList<>();

        // Reactivate metrics
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.AvgSpendOvertime);
        TimeFilter filter = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Year.name(),
                Collections.singletonList(3));
        List<TimeFilter> filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        return metricsList;
    }

    private List<ActivityMetrics> constructMetricsList5() {
        List<ActivityMetrics> metricsList = new ArrayList<>();

        // New TotalSpendOvertime using BETWEEN as relation
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.TotalSpendOvertime);
        TimeFilter filter = new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Year.name(),
                Arrays.asList(3, 5));
        List<TimeFilter> filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        // Update AvgSpendOvertime using BETWEEN as relation
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.AvgSpendOvertime);
        filter = new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Quarter.name(), Arrays.asList(2, 2));
        filters = new ArrayList<>();
        filters.add(filter);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        return metricsList;
    }

    private void verifyTotalAvgSPPeriodConfig(ActivityMetrics metrics, ComparisonType expectedRelation,
            boolean withinToBetween) {
        Assert.assertEquals(metrics.getPeriodsConfig().get(0).getRelation(), expectedRelation);
        switch (expectedRelation) {
        case BETWEEN:
            Assert.assertEquals(metrics.getPeriodsConfig().get(0).getValues().size(), 2);
            if (withinToBetween) {
                Assert.assertEquals(metrics.getPeriodsConfig().get(0).getValues().get(0), 1);
            }
            break;
        case WITHIN:
            Assert.assertEquals(metrics.getPeriodsConfig().get(0).getValues().size(), 1);
            break;
        default:
            throw new IllegalArgumentException(
                    "Unknown expectedRelation for total/avg spend over time " + expectedRelation);
        }

    }

}
