package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_ACTIVE_CONTACTS;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_ALERT_DATA;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_PAGE_NAME;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_PAGE_VISITS;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_RE_ENGAGED_CONTACTS;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_TITLES;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.COL_TITLE_CNT;

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
        Assert.assertEquals(defaults.size(), 8);

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
                "<span class=\"item-insights-bold\">10 visits</span> to the <span class=\"item-insights-bold\">About Us</span> page have occurred meaning they are looking to buy, check your <span class=\"item-insights-bold\">2 active</span> contacts.");

        alertConfig = defaults.stream().filter(a -> a.getAlertHeader().equals("Anonymous Web Visits")).findFirst()
                .orElse(null);
        Assert.assertNotNull(alertConfig);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "Your web pages had <span class=\"item-insights-bold\">10 anonymous visits</span> from this company in the last 10 days.");

        alertConfig = defaults.stream().filter(a -> a.getAlertHeader().equals("Known Web Visits")).findFirst()
                .orElse(null);
        Assert.assertNotNull(alertConfig);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "You have <span class=\"item-insights-bold\">2 known contacts</span> among the visitors that are engaging with you.");

        verifyReEngagedAlertConfig(defaults);
        verifyAcitiveContactsAndWebVisits(defaults);
        verifyIntentAroundProductPages(defaults);
    }

    private void verifyAcitiveContactsAndWebVisits(List<ActivityAlertsConfig> configs) {
        Map<String, Object> input = new HashMap<>();
        Map<String, Object> data = new HashMap<>();
        data.put(COL_ACTIVE_CONTACTS, 5);
        data.put(COL_TITLE_CNT, 3);
        data.put(COL_TITLES, "IT Director,Marketing Ops Manager,IT Admin");
        input.put(COL_ALERT_DATA, data);

        ActivityAlertsConfig alertConfig = configs.stream() //
                .filter(a -> a.getName().equals(ActivityStoreConstants.Alert.ACTIVE_CONTACT_WEB_VISITS)) //
                .findFirst() //
                .orElse(null);
        Assert.assertNotNull(alertConfig);
        String rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "You have <span class=\"item-insights-bold\">5 active contacts</span> at this company. Their titles are <span class=\"item-insights-bold\">IT Director,Marketing Ops Manager,IT Admin</span>.");

        data.put(COL_ACTIVE_CONTACTS, 1);
        data.put(COL_TITLE_CNT, 0);
        input.put(COL_ALERT_DATA, data);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "You have <span class=\"item-insights-bold\">1 active contact</span> at this company.");

        data.put(COL_ACTIVE_CONTACTS, 2);
        data.put(COL_TITLE_CNT, 1);
        data.put(COL_TITLES, "Engineer");
        input.put(COL_ALERT_DATA, data);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "You have <span class=\"item-insights-bold\">2 active contacts</span> at this company. The contacts title is <span class=\"item-insights-bold\">Engineer</span>.");
    }

    private void verifyIntentAroundProductPages(List<ActivityAlertsConfig> configs) {
        Map<String, Object> input = new HashMap<>();
        Map<String, Object> data = new HashMap<>();
        data.put(COL_PAGE_NAME, "Database Product");
        input.put(COL_ALERT_DATA, data);

        ActivityAlertsConfig alertConfig = configs.stream() //
                .filter(a -> a.getName().equals(ActivityStoreConstants.Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES)) //
                .findFirst() //
                .orElse(null);
        Assert.assertNotNull(alertConfig);
        String rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "This company is doing web searches around your <span class=\"item-insights-bold\">Database Product</span>. It looks like they are in a <span class=\"item-insights-bold\">Buying Stage</span>.");

        alertConfig = configs.stream() //
                .filter(a -> a.getName().equals(ActivityStoreConstants.Alert.RESEARCHING_INTENT_AROUND_PRODUCT_PAGES)) //
                .findFirst() //
                .orElse(null);
        Assert.assertNotNull(alertConfig);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "This company is doing web searches around your <span class=\"item-insights-bold\">Database Product</span>. It looks like they are in a <span class=\"item-insights-bold\">Researching Stage</span>.");
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
                "<span class=\"item-insights-bold\">Someone</span> has re-engaged with your product after not returning for more than 30 days.");

        data.put(COL_RE_ENGAGED_CONTACTS, 2L);
        rendered = TemplateUtils.renderByMap(alertConfig.getAlertMessageTemplate(), input);
        Assert.assertEquals(rendered,
                "<span class=\"item-insights-bold\">Multiple contacts</span> have re-engaged with your product after not returning for more than 30 days.");
    }
}
