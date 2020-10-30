package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_ACTIVE_CONTACTS;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_ALERT_DATA;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_NUM_BUY_INTENTS;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_NUM_RESEARCH_INTENTS;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_PAGE_NAME;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_PAGE_VISITS;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_RE_ENGAGED_CONTACTS;

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
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants;

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
        data.put(COL_PAGE_VISITS, 10);
        data.put(COL_PAGE_NAME, "About Us");
        data.put(COL_ACTIVE_CONTACTS, 2);
        input.put(COL_ALERT_DATA, data);
        String rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "10 visits to the About Us page have occurred meaning they are looking to buy, check your 2 active contacts.");

        alertConfig = defaults.stream().filter(a -> a.getAlertHeader().equals("Increased Activity on Product Pages"))
                .findFirst().orElse(null);
        Assert.assertNotNull(alertConfig);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "10 visits to your About Us page suggests you'll need to prospect into those personas.");

        verifyReEngagedAlertConfig(defaults);
        verifyHasShownIntentAlertConfig(defaults);
    }

    private void verifyReEngagedAlertConfig(List<ActivityAlertsConfig> configs) {
        ActivityAlertsConfig alertConfig = configs.stream() //
                .filter(a -> a.getName().equals(ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY)) //
                .findFirst() //
                .orElse(null);
        Assert.assertNotNull(alertConfig);

        Map<String, Object> input = new HashMap<>();
        Map<String, Object> data = new HashMap<>();
        input.put(COL_ALERT_DATA, data);

        // different alert messages for one/multiple contact

        data.put(COL_RE_ENGAGED_CONTACTS, 1L);
        String rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "Someone has re-engaged with your product after not returning for more than 30 days.");

        data.put(COL_RE_ENGAGED_CONTACTS, 2L);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "Multiple contacts have re-engaged with your product after not returning for more than 30 days.");
    }

    private void verifyHasShownIntentAlertConfig(List<ActivityAlertsConfig> defaults) {
        ActivityAlertsConfig alertConfig = defaults.stream() //
                .filter(a -> a.getName().equals(ActivityStoreConstants.Alert.SHOWN_INTENT)) //
                .findFirst() //
                .orElse(null);
        Assert.assertNotNull(alertConfig);

        Map<String, Object> input = new HashMap<>();
        Map<String, Object> data = new HashMap<>();
        input.put(COL_ALERT_DATA, data);

        // have both
        data.put(COL_NUM_BUY_INTENTS, 10L);
        data.put(COL_NUM_RESEARCH_INTENTS, 5L);
        String rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered, "We saw research intent on 5 products and buy intent "
                + "on 10 products within the last 10 days. Check out which models in the Recent Activity section.");

        // no research intent
        data.put(COL_NUM_BUY_INTENTS, 10L);
        data.put(COL_NUM_RESEARCH_INTENTS, 0L);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered, "We saw buy intent on 10 products within the last 10 days."
                + " Check out which models in the Recent Activity section.");

        // no buy intent
        data.put(COL_NUM_BUY_INTENTS, 0L);
        data.put(COL_NUM_RESEARCH_INTENTS, 5L);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered, "We saw research intent on 5 products within the last 10 days."
                + " Check out which models in the Recent Activity section.");
    }
}
