package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ActivityAlertsConfigEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;

public class ActivityAlertsConfigEntityMgrTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActivityAlertsConfigEntityMgrTestNG.class);

    private static final String TEST_ALERT_NAME = "AlertName";

    @Inject
    private ActivityAlertsConfigEntityMgr activityAlertsConfigEntityMgr;

    private RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5);

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCreate() throws InterruptedException {
        ActivityAlertsConfig testConfig = new ActivityAlertsConfig();
        testConfig.setActive(true);
        testConfig.setName(TEST_ALERT_NAME);
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
        Assert.assertEquals(alerts.get(0).getName(), TEST_ALERT_NAME);

        ActivityAlertsConfig alertsConfig = activityAlertsConfigEntityMgr.findByPid(alerts.get(0).getPid());
        Assert.assertNotNull(alertsConfig);
        Assert.assertEquals(alertsConfig.getName(), TEST_ALERT_NAME);

        alertsConfig.setAlertHeader("New Header");
        alertsConfig.setAlertMessageTemplate("New template");
        activityAlertsConfigEntityMgr.createOrUpdate(alertsConfig);

        Thread.sleep(1000L);
        retryTemplate.execute(ctx -> {
            ActivityAlertsConfig updatedConfig = activityAlertsConfigEntityMgr.findByPid(alerts.get(0).getPid());
            Assert.assertNotNull(updatedConfig);
            Assert.assertEquals(updatedConfig.getName(), TEST_ALERT_NAME);
            Assert.assertEquals(updatedConfig.getAlertHeader(), "New Header");
            Assert.assertEquals(updatedConfig.getAlertMessageTemplate(), "New template");
            return null;
        });

        activityAlertsConfigEntityMgr.delete(alertsConfig);
        Thread.sleep(1000L);
        retryTemplate.execute(ctx -> {
            ActivityAlertsConfig deletedConfig = activityAlertsConfigEntityMgr.findByPid(alerts.get(0).getPid());
            Assert.assertNull(deletedConfig);
            return null;
        });
    }
}
