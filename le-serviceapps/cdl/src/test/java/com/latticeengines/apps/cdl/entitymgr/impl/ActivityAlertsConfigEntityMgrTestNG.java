package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ActivityAlertsConfigEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;

public class ActivityAlertsConfigEntityMgrTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActivityAlertsConfigEntityMgrTestNG.class);

    @Inject
    private ActivityAlertsConfigEntityMgr activityAlertsConfigEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void TestCrud() throws InterruptedException {
        ActivityAlertsConfig testConfig = new ActivityAlertsConfig();
        String id = UUID.randomUUID().toString();
        testConfig.setActive(true);
        testConfig.setId(id);
        testConfig.setAlertMessageTemplate("Test ${alert.page_visits} to ${alert.page_name} page.");
        testConfig.setAlertHeader("Alert Header");
        testConfig.setAlertCategory(AlertCategory.PRODUCTS);
        testConfig.setQualificationPeriodDays(10);
        testConfig.setTenant(mainTestTenant);
        activityAlertsConfigEntityMgr.createOrUpdate(testConfig);

        String json = JsonUtils.serialize(testConfig);
        Map<String, Object> data = new HashMap<>();
        data.put("page_visits", 10);
        data.put("page_name", "About Us");
        Map<String, Object> input = new HashMap<>();
        input.put("alert", data);
        String rendered = TemplateUtils.renderByMap(testConfig.getAlertMessageTemplate(), input);

        List<ActivityAlertsConfig> alerts = activityAlertsConfigEntityMgr.findAllByTenant(mainTestTenant);
        Assert.assertTrue(CollectionUtils.isNotEmpty(alerts));
        Assert.assertEquals(alerts.size(), 1);
        Assert.assertEquals(alerts.get(0).getId(), id);

        ActivityAlertsConfig alertsConfig = activityAlertsConfigEntityMgr.findByPid(alerts.get(0).getPid());
        Assert.assertNotNull(alertsConfig);
        Assert.assertEquals(alertsConfig.getId(), id);

        alertsConfig.setAlertHeader("New Header");
        alertsConfig.setAlertMessageTemplate("New template");
        activityAlertsConfigEntityMgr.createOrUpdate(alertsConfig);

        alertsConfig = activityAlertsConfigEntityMgr.findByPid(alerts.get(0).getPid());
        Assert.assertNotNull(alertsConfig);
        Assert.assertEquals(alertsConfig.getId(), id);
        Assert.assertEquals(alertsConfig.getAlertHeader(), "New Header");
        Assert.assertEquals(alertsConfig.getAlertMessageTemplate(), "New template");

        activityAlertsConfigEntityMgr.delete(alertsConfig);
        alertsConfig = activityAlertsConfigEntityMgr.findByPid(alerts.get(0).getPid());
        Thread.sleep(2000L);
        Assert.assertNull(alertsConfig);

    }
}
