package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
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
        });

        List<PurchaseMetrics> found = entityMgr.findAll();
        Assert.assertFalse(CollectionUtils.isEmpty(found));
        Assert.assertEquals(found.size(), 2);
        found.forEach(item -> {
            Assert.assertNotNull(item.getPeriodsConfig());
        });

        Tenant tenant = new Tenant("dummy");
        tenant.setPid(-1L);
        MultiTenantContext.setTenant(tenant);
        List<PurchaseMetrics> list = entityMgr.findAll();
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

}
