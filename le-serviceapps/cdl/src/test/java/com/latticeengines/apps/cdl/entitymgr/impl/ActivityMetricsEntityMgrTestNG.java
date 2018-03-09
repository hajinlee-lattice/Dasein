package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsEntityMgrTestNG extends CDLFunctionalTestNGBase {
    @Inject
    private ActivityMetricsEntityMgr entityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testMultiTenantFilter() {
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findWithType(ActivityType.PurchaseHistory)));

        List<ActivityMetrics> metricsList = constructMetricsList();

        List<ActivityMetrics> saved = entityMgr.save(metricsList);
        Assert.assertFalse(CollectionUtils.isEmpty(saved));
        saved.forEach(metrics -> {
            Assert.assertNotNull(metrics.getPid());
            Assert.assertFalse(metrics.isEOL());
            Assert.assertNull(metrics.getDeprecated());
            Assert.assertNotNull(metrics.getTenant());
        });

        List<ActivityMetrics> active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertFalse(CollectionUtils.isEmpty(active));
        Assert.assertEquals(active.size(), 2);
        active.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
        });

        metricsList = constructUpdatedMetricsList();
        saved = entityMgr.save(metricsList);
        Assert.assertFalse(CollectionUtils.isEmpty(saved));
        Assert.assertEquals(saved.size(), 3);

        active = entityMgr.findActiveWithType(ActivityType.PurchaseHistory);
        Assert.assertFalse(CollectionUtils.isEmpty(active));
        Assert.assertEquals(active.size(), 2);

        List<ActivityMetrics> all = entityMgr.findWithType(ActivityType.PurchaseHistory);
        Assert.assertFalse(CollectionUtils.isEmpty(all));
        Assert.assertEquals(all.size(), 3);

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
        TimeFilter filter1 = new TimeFilter(ComparisonType.WITHIN, Period.Month.name(), Collections.singletonList(1));
        TimeFilter filter2 = new TimeFilter(ComparisonType.PRIOR_ONLY, Period.Month.name(),
                Collections.singletonList(2));
        List<TimeFilter> filters = new ArrayList<>();
        filters.add(filter1);
        filters.add(filter2);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.TotalSpendOvertime);
        TimeFilter filter3 = new TimeFilter(ComparisonType.WITHIN, Period.Month.name(), Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter3);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);
        return metricsList;
    }

    private List<ActivityMetrics> constructUpdatedMetricsList() {
        List<ActivityMetrics> metricsList = new ArrayList<>();
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.Margin);
        TimeFilter filter1 = new TimeFilter(ComparisonType.WITHIN, Period.Month.name(), Collections.singletonList(1));
        List<TimeFilter> filters = new ArrayList<>();
        filters.add(filter1);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);
        metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.TotalSpendOvertime);
        TimeFilter filter2 = new TimeFilter(ComparisonType.WITHIN, Period.Month.name(), Collections.singletonList(1));
        filters = new ArrayList<>();
        filters.add(filter2);
        metrics.setPeriodsConfig(filters);
        metrics.setType(ActivityType.PurchaseHistory);
        metricsList.add(metrics);
        return metricsList;
    }

}
