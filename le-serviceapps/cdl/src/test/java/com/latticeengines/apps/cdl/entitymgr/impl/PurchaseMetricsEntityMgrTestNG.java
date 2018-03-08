package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PurchaseMetricsEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.PurchaseMetrics;

public class PurchaseMetricsEntityMgrTestNG extends CDLFunctionalTestNGBase {
    @Inject
    private PurchaseMetricsEntityMgr entityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testMultiTenantFilter() {
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findAll()));

        List<PurchaseMetrics> metricsList = constructMetricsList();

        List<PurchaseMetrics> saved = entityMgr.save(metricsList);
        Assert.assertFalse(CollectionUtils.isEmpty(saved));
        saved.forEach(metrics -> {
            Assert.assertNotNull(metrics.getPid());
            Assert.assertFalse(metrics.isEOL());
            Assert.assertNull(metrics.getDeprecated());
            Assert.assertNotNull(metrics.getTenant());
        });

        List<PurchaseMetrics> active = entityMgr.findAllActive();
        Assert.assertFalse(CollectionUtils.isEmpty(active));
        Assert.assertEquals(active.size(), 2);
        active.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
        });

        metricsList = constructUpdatedMetricsList();
        saved = entityMgr.save(metricsList);
        Assert.assertFalse(CollectionUtils.isEmpty(saved));
        Assert.assertEquals(saved.size(), 3);

        active = entityMgr.findAllActive();
        Assert.assertFalse(CollectionUtils.isEmpty(active));
        Assert.assertEquals(active.size(), 2);

        List<PurchaseMetrics> all = entityMgr.findAll();
        Assert.assertFalse(CollectionUtils.isEmpty(all));
        Assert.assertEquals(all.size(), 3);

        Tenant tenant = new Tenant("dummy");
        tenant.setPid(-1L);
        MultiTenantContext.setTenant(tenant);
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findAllActive()));
        Assert.assertTrue(CollectionUtils.isEmpty(entityMgr.findAll()));
    }

    private List<PurchaseMetrics> constructMetricsList() {
        List<PurchaseMetrics> metricsList = new ArrayList<>();
        PurchaseMetrics metrics = new PurchaseMetrics();
        metrics.setMetrics(InterfaceName.SpendChange);
        TimeFilter filter = new TimeFilter(ComparisonType.WITHIN, Period.Month.name(), Collections.singletonList(1));
        metrics.setPeriodsConfig(filter);
        metricsList.add(metrics);
        metrics = new PurchaseMetrics();
        metrics.setMetrics(InterfaceName.SpendOvertime);
        filter = new TimeFilter(ComparisonType.BETWEEN, Period.Month.name(), Arrays.asList(1, 2));
        metrics.setPeriodsConfig(filter);
        metricsList.add(metrics);
        return metricsList;
    }

    private List<PurchaseMetrics> constructUpdatedMetricsList() {
        List<PurchaseMetrics> metricsList = new ArrayList<>();
        PurchaseMetrics metrics = new PurchaseMetrics();
        metrics.setMetrics(InterfaceName.Margin);
        TimeFilter filter = new TimeFilter(ComparisonType.WITHIN, Period.Month.name(), Collections.singletonList(1));
        metrics.setPeriodsConfig(filter);
        metricsList.add(metrics);
        metrics = new PurchaseMetrics();
        metrics.setMetrics(InterfaceName.SpendOvertime);
        filter = new TimeFilter(ComparisonType.BETWEEN, Period.Month.name(), Arrays.asList(1, 2));
        metrics.setPeriodsConfig(filter);
        metricsList.add(metrics);
        return metricsList;
    }

}
