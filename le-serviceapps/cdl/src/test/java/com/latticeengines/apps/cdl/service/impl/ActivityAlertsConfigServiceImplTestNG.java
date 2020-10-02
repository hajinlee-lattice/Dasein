package com.latticeengines.apps.cdl.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ActivityAlertsConfigService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;

public class ActivityAlertsConfigServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private ActivityAlertsConfigService activityAlertsConfigService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testDefaultConfigSetup() {

        List<ActivityAlertsConfig> defaults = activityAlertsConfigService.findAllByTenant(mainCustomerSpace);
        Assert.assertTrue(CollectionUtils.isEmpty(defaults));

        defaults = activityAlertsConfigService.createDefaultActivityAlertsConfigs(mainCustomerSpace);
        Assert.assertNotNull(defaults);
        Assert.assertEquals(defaults.size(), 4);

        verifyDefaultAlertConfig(defaults);

    }

    private void verifyDefaultAlertConfig(List<ActivityAlertsConfig> defaults) {
        Map<String, Object> input = new HashMap<>();
        Map<String, Object> data = new HashMap<>();

        ActivityAlertsConfig alertConfig = defaults.stream()
                .filter(a -> a.getAlertHeader().equals("Increased Web Activity")).findFirst().orElse(null);
        Assert.assertNotNull(alertConfig);
        data.put("page_visits", 10);
        data.put("page_name", "About Us");
        data.put("active_contacts", 2);
        input.put("alert", data);
        String rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "There have been 10 visits to the About Us pages. Check on this interest with your 2 active contacts. This may mean they are looking to buy.");

        alertConfig = defaults.stream().filter(a -> a.getAlertHeader().equals("Increased Activity on Product Pages"))
                .findFirst().orElse(null);
        Assert.assertNotNull(alertConfig);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "There have been 10 visits to the About Us pages. This means you'll need to prospect more into the personas.");

        alertConfig = defaults.stream().filter(a -> a.getAlertHeader().equals("Re-engaged Activity")).findFirst()
                .orElse(null);
        Assert.assertNotNull(alertConfig);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "Someone from this account has visited a product page, after not visiting for more than 30 days. This may mean they are looking to reinitiate their search.");

        alertConfig = defaults.stream().filter(a -> a.getAlertHeader().equals("Growing Buyer Intent")).findFirst()
                .orElse(null);
        Assert.assertNotNull(alertConfig);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "Growing buyer intent happening from this account in the last 10 days with 10 amount of Product URL clicks. This may mean they are looking to buy.");

        alertConfig = defaults.stream().filter(a -> a.getAlertHeader().equals("Growing Research Intent")).findFirst()
                .orElse(null);
        Assert.assertNotNull(alertConfig);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "Growing research intent happening from this account in the last 10 days with 10 amount of Product URL clicks. This may mean they are looking for more product information.");
    }
}
