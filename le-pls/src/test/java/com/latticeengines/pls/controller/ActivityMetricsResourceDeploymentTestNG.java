package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetricsValidation;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class ActivityMetricsResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsResourceDeploymentTestNG.class);

    @Inject
    private ActionService actionService;

    @Inject
    private CDLTestDataService cdlTestDataService;

    private List<ActivityMetrics> created, updated, secUpdated;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);

        created = constructMetricsList();
        updated = constructUpdatedMetricsList();
        secUpdated = constructSecUpdatedMetricsList();
    }

    // Create 2 new metrics
    @Test(groups = "deployment", priority = 1)
    public void testCreate() {
        List<?> list = restTemplate.postForObject(getRestAPIHostPort() + "/pls/datacollection/metrics/PurchaseHistory", created,
                List.class);
        List<ActivityMetrics> saved = JsonUtils.convertList(list, ActivityMetrics.class);
        Assert.assertNotNull(saved);
        Assert.assertEquals(saved.size(), created.size());

        list = restTemplate.getForObject(getRestAPIHostPort() + "/pls/datacollection/metrics/PurchaseHistory/active",
                List.class);
        List<ActivityMetrics> active = JsonUtils.convertList(list, ActivityMetrics.class);
        Assert.assertNotNull(active);
        Assert.assertEquals(active.size(), created.size());

        verifyActions(1);
    }

    // Update 1 metrics (new 1 & deprecate 1), Create 1 metrics, Retain 1 metrics
    @Test(groups = "deployment", priority = 2)
    public void testUpdate() {
        List<?> list = restTemplate.postForObject(getRestAPIHostPort() + "/pls/datacollection/metrics/PurchaseHistory",
                updated, List.class);
        List<ActivityMetrics> saved = JsonUtils.convertList(list, ActivityMetrics.class);
        Assert.assertNotNull(saved);
        Assert.assertEquals(saved.size(), updated.size());

        list = restTemplate.getForObject(getRestAPIHostPort() + "/pls/datacollection/metrics/PurchaseHistory/active",
                List.class);
        List<ActivityMetrics> active = JsonUtils.convertList(list, ActivityMetrics.class);
        Assert.assertNotNull(active);
        Assert.assertEquals(active.size(), updated.size());

        verifyActions(2);
    }

    // Re-activate 1 old metrics, deprecate all the other metrics
    @Test(groups = "deployment", priority = 3)
    public void testReactivate() {
        List<?> list = restTemplate.postForObject(getRestAPIHostPort() + "/pls/datacollection/metrics/PurchaseHistory",
                secUpdated, List.class);
        List<ActivityMetrics> saved = JsonUtils.convertList(list, ActivityMetrics.class);
        Assert.assertNotNull(saved);
        Assert.assertEquals(saved.size(), secUpdated.size());

        list = restTemplate.getForObject(getRestAPIHostPort() + "/pls/datacollection/metrics/PurchaseHistory/active",
                List.class);
        List<ActivityMetrics> active = JsonUtils.convertList(list, ActivityMetrics.class);
        Assert.assertNotNull(active);
        Assert.assertEquals(active.size(), secUpdated.size());

        verifyActions(3);
    }

    @Test(groups = "deployment")
    public void testPrecheck() {
        ActivityMetricsValidation validation = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/datacollection/metrics/precheck", ActivityMetricsValidation.class);
        Assert.assertNotNull(validation);
        Assert.assertTrue(validation.getDisableAll());
        Assert.assertTrue(validation.getDisableShareOfWallet());
        Assert.assertTrue(validation.getDisableMargin());

        cdlTestDataService.populateData(mainTestTenant.getId());
        validation = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/datacollection/metrics/precheck", ActivityMetricsValidation.class);
        Assert.assertNotNull(validation);
        Assert.assertTrue(validation.getDisableAll());
        Assert.assertTrue(validation.getDisableShareOfWallet());
        Assert.assertTrue(validation.getDisableMargin());
    }

    private void verifyActions(int cnt) {
        List<Action> actions = actionService.findAll();
        Assert.assertNotNull(actions);
        Assert.assertEquals(actions.size(), cnt);
        actions.forEach(action -> {
            Assert.assertNotNull(action);
            Assert.assertEquals(action.getType(), ActionType.ACTIVITY_METRICS_CHANGE);
            Assert.assertNotNull(action.getActionInitiator());
            Assert.assertTrue(action.getActionInitiator().contains("@"));
            Assert.assertNotNull(action.getDescription());
            log.info("ActivityMetricsChange description is " + action.getDescription());
        });
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
        metrics.setMetrics(InterfaceName.ShareOfWallet);
        TimeFilter filter3 = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter3);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        return metricsList;
    }

    private List<ActivityMetrics> constructUpdatedMetricsList() {
        // Updated
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

        // Same
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.ShareOfWallet);
        TimeFilter filter3 = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter3);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        // New
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.Margin);
        TimeFilter filter4 = new TimeFilter(ComparisonType.WITHIN, PeriodStrategy.Template.Week.name(),
                Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter4);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);

        return metricsList;
    }

    private List<ActivityMetrics> constructSecUpdatedMetricsList() {
        // Re-activated
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

        return metricsList;
    }
}
