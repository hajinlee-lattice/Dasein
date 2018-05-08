package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class AttrConfigServiceImplUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImplUnitTestNG.class);

    @Mock
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @InjectMocks
    @Spy
    private AttrConfigServiceImpl attrConfigService;

    private static final Long intentLimit = 500L;
    private static final Long activeForIntent = 5000L;
    private static final Long inactiveForIntent = 4000L;
    private static final Long totalIntentAttrs = 90000L;
    private static final Long tpLimit = 500L;
    private static final Long activeForTp = 5000L;
    private static final Long inactiveForTp = 4000L;
    private static final Long totalTpAttrs = 90000L;
    private static final Long accountLimit = 500L;
    private static final Long activeForAccount = 5000L;
    private static final Long inactiveForAccount = 4000L;
    private static final Long totalAccountAttrs = 90000L;
    private static final Long contactLimit = 500L;
    private static final Long activeForContact = 5000L;
    private static final Long inactiveForContact = 4000L;
    private static final Long totalContactAttrs = 90000L;
    private static Tenant tenant;

    private static final String[] select = { "attr1", "attr2", "attr3" };
    private static final String[] deselect = { "attr4", "attr5", "attr6" };
    private static final String usage = "Export";

    @BeforeTest(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        tenant = new Tenant("tenantId");
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "unit")
    public void testGetAttrConfigActivationOverview() {
        when(cdlAttrConfigProxy.getAttrConfigOverview(anyString(), anyString(), anyString()))
                .thenReturn(Arrays.asList(generateIntentAttrConfigOverview()));
        AttrConfigActivationOverview categoryOverview = attrConfigService
                .getAttrConfigActivationOverview(Category.INTENT);
        Assert.assertEquals(categoryOverview.getTotalAttrs(), totalIntentAttrs);
        Assert.assertEquals(categoryOverview.getCategory(), Category.INTENT);
        Assert.assertEquals(categoryOverview.getLimit(), intentLimit);
        Assert.assertEquals(categoryOverview.getSelected(), activeForIntent);
    }

    @Test(groups = "unit")
    public void testGetOverallAttrConfigActivationOverview() {
        when(cdlAttrConfigProxy.getAttrConfigOverview(anyString(), Matchers.anyList(), Matchers.anyList(),
                anyBoolean())).thenReturn(generatePremiumCategoryAttrConfigActivationOverview());
        Map<String, AttrConfigActivationOverview> result = attrConfigService.getOverallAttrConfigActivationOverview();
        Assert.assertEquals(result.size(), Category.getPremiunCategories().size());

        AttrConfigActivationOverview categoryOverview = result.get(Category.INTENT.getName());
        Assert.assertEquals(categoryOverview.getTotalAttrs(), totalIntentAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), intentLimit);
        Assert.assertEquals(categoryOverview.getSelected(), activeForIntent);

        categoryOverview = result.get(Category.TECHNOLOGY_PROFILE.getName());
        Assert.assertEquals(categoryOverview.getTotalAttrs(), totalTpAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), tpLimit);
        Assert.assertEquals(categoryOverview.getSelected(), activeForTp);

        categoryOverview = result.get(Category.ACCOUNT_ATTRIBUTES.getName());
        Assert.assertEquals(categoryOverview.getTotalAttrs(), totalAccountAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), accountLimit);
        Assert.assertEquals(categoryOverview.getSelected(), activeForAccount);

        categoryOverview = result.get(Category.CONTACT_ATTRIBUTES.getName());
        Assert.assertEquals(categoryOverview.getTotalAttrs(), totalContactAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), contactLimit);
        Assert.assertEquals(categoryOverview.getSelected(), activeForContact);

    }

    @Test(groups = "unit", dependsOnMethods = { "testGetAttrConfigActivationOverview" })
    public void testGetAttrConfigUsageOverview() {
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                ColumnSelection.Predefined.Segment.getName()))
                        .thenReturn(generatePropertyAttrConfigOverview(ColumnSelection.Predefined.Segment.getName()));
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                ColumnSelection.Predefined.Enrichment.getName())).thenReturn(
                        generatePropertyAttrConfigOverview(ColumnSelection.Predefined.Enrichment.getName()));
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                ColumnSelection.Predefined.CompanyProfile.getName())).thenReturn(
                        generatePropertyAttrConfigOverview(ColumnSelection.Predefined.CompanyProfile.getName()));
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                ColumnSelection.Predefined.TalkingPoint.getName())).thenReturn(
                        generatePropertyAttrConfigOverview(ColumnSelection.Predefined.TalkingPoint.getName()));
        AttrConfigUsageOverview usageOverview = attrConfigService.getAttrConfigUsageOverview();
        log.info("usageOverview is " + usageOverview);
        Map<String, Long> attrNums = usageOverview.getAttrNums();
        Assert.assertEquals(attrNums.size(), 6);
        Assert.assertEquals(attrNums.get(Category.INTENT.getName()) - activeForIntent, 0);
        Map<String, Map<String, Long>> selections = usageOverview.getSelections();
        Assert.assertEquals(
                selections.get(ColumnSelection.Predefined.Segment.getName()).get(AttrConfigUsageOverview.SELECTED)
                        - 3677,
                0);
        Assert.assertNotNull(
                selections.get(ColumnSelection.Predefined.Enrichment.getName()).get(AttrConfigUsageOverview.LIMIT));
    }

    @Test(groups = "unit", dependsOnMethods = { "testGetOverallAttrConfigActivationOverview" })
    public void testGetOverallAttrConfigUsageOverview() {
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                Arrays.asList(AttrConfigServiceImpl.usageProperties), true))
                        .thenReturn(generatePropertyAttrConfigOverviewForUsage(
                                Arrays.asList(AttrConfigServiceImpl.usageProperties)));
        AttrConfigUsageOverview usageOverview = attrConfigService.getOverallAttrConfigUsageOverview();
        log.info("overall usageOverview is " + usageOverview);
        Map<String, Long> attrNums = usageOverview.getAttrNums();
        Assert.assertEquals(attrNums.size(), 6);
        Assert.assertEquals(attrNums.get(Category.INTENT.getName()) - 10960L, 0);
        Map<String, Map<String, Long>> selections = usageOverview.getSelections();
        Assert.assertEquals(
                selections.get(ColumnSelection.Predefined.Segment.getName()).get(AttrConfigUsageOverview.SELECTED)
                        - 3677,
                0);
        Assert.assertNotNull(
                selections.get(ColumnSelection.Predefined.Enrichment.getName()).get(AttrConfigUsageOverview.LIMIT));
    }

    @Test(groups = "unit")
    public void testTranslateUsageToProperty() {
        Assert.assertTrue(attrConfigService.translateUsageToProperty("SEGMENTATION")
                .equals(ColumnSelection.Predefined.Segment.getName()));
        Assert.assertTrue(attrConfigService.translateUsageToProperty("EXPoRT")
                .equals(ColumnSelection.Predefined.Enrichment.getName()));
        Assert.assertTrue(attrConfigService.translateUsageToProperty("TALKING pOINTS")
                .equals(ColumnSelection.Predefined.TalkingPoint.getName()));
        Assert.assertTrue(attrConfigService.translateUsageToProperty("COMPANY PROFILE")
                .equals(ColumnSelection.Predefined.CompanyProfile.getName()));
        Assert.assertTrue(attrConfigService.translateUsageToProperty("State").equals(ColumnMetadataKey.State));
        try {
            attrConfigService.translateUsageToProperty("randome");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test(groups = "unit")
    public void testGenerateAttrConfigRequestForUsage() {
        AttrConfigSelectionRequest request = new AttrConfigSelectionRequest();
        request.setDeselect(Arrays.asList(deselect));
        request.setSelect(Arrays.asList(select));
        AttrConfigRequest attrConfigRequest = attrConfigService.generateAttrConfigRequestForUsage(usage, request);
        List<AttrConfig> attrConfigs = attrConfigRequest.getAttrConfigs();
        Assert.assertEquals(attrConfigs.size(), select.length + deselect.length);
        for (AttrConfig attrConfig : attrConfigs) {
            Assert.assertNotNull(attrConfig.getAttrName());
            Assert.assertEquals(attrConfig.getEntity(), BusinessEntity.Account);
            Assert.assertTrue(attrConfig.getAttrProps().containsKey(ColumnSelection.Predefined.Enrichment.getName()));
            log.info("attrConfig is " + JsonUtils.serialize(attrConfig));
        }
    }

    @Test(groups = "unit")
    public void testGenerateAttrConfigRequestForActivation() {
        AttrConfigSelectionRequest request = new AttrConfigSelectionRequest();
        request.setDeselect(Arrays.asList(deselect));
        request.setSelect(Arrays.asList(select));
        AttrConfigRequest attrConfigRequest = attrConfigService.generateAttrConfigRequestForActivation(request);
        List<AttrConfig> attrConfigs = attrConfigRequest.getAttrConfigs();
        Assert.assertEquals(attrConfigs.size(), select.length + deselect.length);
        for (AttrConfig attrConfig : attrConfigs) {
            Assert.assertNotNull(attrConfig.getAttrName());
            Assert.assertEquals(attrConfig.getEntity(), BusinessEntity.Account);
            Assert.assertTrue(attrConfig.getAttrProps().containsKey(ColumnMetadataKey.State));
            log.info("attrConfig is " + JsonUtils.serialize(attrConfig));
        }
    }

    @Test(groups = "unit")
    public void testAttrConfig() {
        List<AttrConfig> attrConfigs = new ArrayList<>();
        AttrConfig config = new AttrConfig();
        config.setAttrName("a");
        config.setEntity(BusinessEntity.Account);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(Boolean.TRUE);
        config.setAttrProps(ImmutableMap.of(ColumnMetadataKey.State, enrichProp));
        log.info("isAllowCustomization() is "
                + config.getAttrProps().get(ColumnMetadataKey.State).isAllowCustomization());
        log.info("CustomValue() is " + config.getAttrProps().get(ColumnMetadataKey.State).getCustomValue());
        log.info("SystemValue() is " + config.getAttrProps().get(ColumnMetadataKey.State).getSystemValue());
        attrConfigs.add(config);
    }

    @Test(groups = "unit", dependsOnMethods = { "testGetAttrConfigUsageOverview" })
    public void testGetDetailAttrForActivation() {
        AttrConfigRequest request = new AttrConfigRequest();
        request.setAttrConfigs(Arrays.asList(AttrConfigServiceImplTestUtils.getAttr1(Category.TECHNOLOGY_PROFILE, true),
                AttrConfigServiceImplTestUtils.getAttr2(Category.TECHNOLOGY_PROFILE, true), //
                AttrConfigServiceImplTestUtils.getAttr3(Category.TECHNOLOGY_PROFILE, true), //
                AttrConfigServiceImplTestUtils.getAttr4(Category.TECHNOLOGY_PROFILE, true), //
                AttrConfigServiceImplTestUtils.getAttr5(Category.TECHNOLOGY_PROFILE, false), //
                AttrConfigServiceImplTestUtils.getAttr6(Category.TECHNOLOGY_PROFILE, false), //
                AttrConfigServiceImplTestUtils.getAttr7(Category.TECHNOLOGY_PROFILE, false), //
                AttrConfigServiceImplTestUtils.getAttr8(Category.TECHNOLOGY_PROFILE, false), //
                AttrConfigServiceImplTestUtils.getAttr9(Category.TECHNOLOGY_PROFILE, false)));
        when(cdlAttrConfigProxy.getAttrConfigByCategory(tenant.getId(), Category.TECHNOLOGY_PROFILE.getName()))
                .thenReturn(request);
        AttrConfigSelectionDetail activationDetail = attrConfigService
                .getAttrConfigSelectionDetailForState(Category.TECHNOLOGY_PROFILE.getName());
        log.info("testGetAttrConfigUsageOverview activationDetail is " + activationDetail);
        Assert.assertEquals(activationDetail.getSelected() - 4L, 0);
        Assert.assertEquals(activationDetail.getTotalAttrs() - 9L, 0);
        Assert.assertEquals(activationDetail.getSubcategories().size(), 8);
        Assert.assertTrue(activationDetail.getSubcategories().parallelStream()
                .allMatch(entry -> entry.getHasFrozenAttrs() == false));
    }

    @Test(groups = "unit", dependsOnMethods = { "testGetAttrConfigUsageOverview" })
    public void testGetDetailAttrForUsageWithNonPremiumCategory() {
        AttrConfigRequest request = new AttrConfigRequest();
        request.setAttrConfigs(Arrays.asList(AttrConfigServiceImplTestUtils.getAttr1(Category.FIRMOGRAPHICS, true),
                AttrConfigServiceImplTestUtils.getAttr2(Category.FIRMOGRAPHICS, true), //
                AttrConfigServiceImplTestUtils.getAttr3(Category.FIRMOGRAPHICS, true), //
                AttrConfigServiceImplTestUtils.getAttr4(Category.FIRMOGRAPHICS, true), //
                AttrConfigServiceImplTestUtils.getAttr5(Category.FIRMOGRAPHICS, false), //
                AttrConfigServiceImplTestUtils.getAttr6(Category.FIRMOGRAPHICS, false), //
                AttrConfigServiceImplTestUtils.getAttr7(Category.FIRMOGRAPHICS, false), //
                AttrConfigServiceImplTestUtils.getAttr8(Category.FIRMOGRAPHICS, false), //
                AttrConfigServiceImplTestUtils.getAttr9(Category.FIRMOGRAPHICS, false)));
        when(cdlAttrConfigProxy.getAttrConfigByCategory(tenant.getId(), Category.FIRMOGRAPHICS.getName()))
                .thenReturn(request);
        AttrConfigSelectionDetail selectionDetail = attrConfigService.getAttrConfigSelectionDetails("Firmographics",
                "Segmentation");
        log.info("testGetDetailAttrForUsageWithNonPremiumCategory selectionDetail is " + selectionDetail);
        Assert.assertEquals(selectionDetail.getSelected() - 0L, 0);
        Assert.assertEquals(selectionDetail.getTotalAttrs() - 1L, 0);
        Assert.assertEquals(selectionDetail.getSubcategories().size(), 1);
        Assert.assertEquals(selectionDetail.getSubcategories().parallelStream()
                .filter(entry -> entry.getHasFrozenAttrs() == false).count(), 1);
    }

    @Test(groups = "unit", dependsOnMethods = { "testGetAttrConfigUsageOverview" })
    public void testGetDetailAttrForUsageWithPremiumCategory() {
        AttrConfigRequest request = new AttrConfigRequest();
        request.setAttrConfigs(Arrays.asList(AttrConfigServiceImplTestUtils.getAttr1(Category.INTENT, true, true),
                AttrConfigServiceImplTestUtils.getAttr2(Category.INTENT, true, true), //
                AttrConfigServiceImplTestUtils.getAttr3(Category.INTENT, true, true), //
                AttrConfigServiceImplTestUtils.getAttr4(Category.INTENT, true, false), //
                AttrConfigServiceImplTestUtils.getAttr5(Category.INTENT, false, true), //
                AttrConfigServiceImplTestUtils.getAttr6(Category.INTENT, false, false), //
                AttrConfigServiceImplTestUtils.getAttr7(Category.INTENT, false, false), //
                AttrConfigServiceImplTestUtils.getAttr8(Category.INTENT, false, false), //
                AttrConfigServiceImplTestUtils.getAttr9(Category.INTENT, false, false)));
        when(cdlAttrConfigProxy.getAttrConfigByCategory(tenant.getId(), Category.INTENT.getName())).thenReturn(request);
        AttrConfigSelectionDetail selectionDetail = attrConfigService
                .getAttrConfigSelectionDetails(Category.INTENT.getName(), "Segmentation");
        log.info("testGetDetailAttrForUsageWithPremiumCategoru selectionDetail is " + selectionDetail);
        Assert.assertEquals(selectionDetail.getSelected() - 1L, 0);
        Assert.assertEquals(selectionDetail.getTotalAttrs() - 1L, 0);
        Assert.assertEquals(selectionDetail.getSubcategories().size(), 1);
        Assert.assertEquals(selectionDetail.getSubcategories().parallelStream()
                .filter(entry -> entry.getHasFrozenAttrs() == false).count(), 1);
    }

    private AttrConfigOverview<AttrState> generateIntentAttrConfigOverview() {
        AttrConfigOverview<AttrState> intentCategoryAttrConfigOverview = new AttrConfigOverview<>();
        intentCategoryAttrConfigOverview.setCategory(Category.INTENT);
        intentCategoryAttrConfigOverview.setLimit(intentLimit);
        intentCategoryAttrConfigOverview.setTotalAttrs(totalIntentAttrs);
        Map<String, Map<AttrState, Long>> propSummary = new HashMap<>();
        intentCategoryAttrConfigOverview.setPropSummary(propSummary);
        Map<AttrState, Long> valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForIntent);
        valueCountMap.put(AttrState.Inactive, inactiveForIntent);
        valueCountMap.put(AttrState.Deprecated, 0L);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);
        log.info("intentCategoryAttrConfigOverview is " + intentCategoryAttrConfigOverview);
        return intentCategoryAttrConfigOverview;
    }

    private Map<String, AttrConfigCategoryOverview<?>> generatePremiumCategoryAttrConfigActivationOverview() {
        Map<String, AttrConfigCategoryOverview<?>> map = new HashMap<>();

        AttrConfigCategoryOverview<AttrState> intentCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.INTENT.getName(), intentCategoryAttrConfigOverview);
        intentCategoryAttrConfigOverview.setLimit(intentLimit);
        intentCategoryAttrConfigOverview.setTotalAttrs(totalIntentAttrs);
        Map<String, Map<AttrState, Long>> propSummary = new HashMap<>();
        intentCategoryAttrConfigOverview.setPropSummary(propSummary);
        Map<AttrState, Long> valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForIntent);
        valueCountMap.put(AttrState.Inactive, inactiveForIntent);
        valueCountMap.put(AttrState.Deprecated, 0L);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        AttrConfigCategoryOverview<AttrState> tpCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.TECHNOLOGY_PROFILE.getName(), tpCategoryAttrConfigOverview);
        tpCategoryAttrConfigOverview.setLimit(tpLimit);
        tpCategoryAttrConfigOverview.setTotalAttrs(totalTpAttrs);
        propSummary = new HashMap<>();
        tpCategoryAttrConfigOverview.setPropSummary(propSummary);
        valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForTp);
        valueCountMap.put(AttrState.Inactive, inactiveForTp);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        AttrConfigCategoryOverview<AttrState> accountCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.ACCOUNT_ATTRIBUTES.getName(), accountCategoryAttrConfigOverview);
        accountCategoryAttrConfigOverview.setLimit(accountLimit);
        accountCategoryAttrConfigOverview.setTotalAttrs(totalAccountAttrs);
        propSummary = new HashMap<>();
        accountCategoryAttrConfigOverview.setPropSummary(propSummary);
        valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForAccount);
        valueCountMap.put(AttrState.Inactive, inactiveForAccount);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        AttrConfigCategoryOverview<AttrState> contactCategoryAttrConfigOverview = new AttrConfigCategoryOverview<>();
        map.put(Category.CONTACT_ATTRIBUTES.getName(), contactCategoryAttrConfigOverview);
        contactCategoryAttrConfigOverview.setLimit(contactLimit);
        contactCategoryAttrConfigOverview.setTotalAttrs(totalContactAttrs);
        propSummary = new HashMap<>();
        contactCategoryAttrConfigOverview.setPropSummary(propSummary);
        valueCountMap = new HashMap<>();
        valueCountMap.put(AttrState.Active, activeForContact);
        valueCountMap.put(AttrState.Inactive, inactiveForContact);
        propSummary.put(ColumnMetadataKey.State, valueCountMap);

        log.info("map is " + map);
        return map;
    }

    private Map<String, AttrConfigCategoryOverview<?>> generatePropertyAttrConfigOverviewForUsage(
            List<String> propertyNames) {
        Map<String, AttrConfigCategoryOverview<?>> result = new HashMap<>();
        AttrConfigCategoryOverview<Boolean> attrConfig1 = new AttrConfigCategoryOverview<>();
        result.put(Category.FIRMOGRAPHICS.getName(), attrConfig1);
        attrConfig1.setLimit(500L);
        attrConfig1.setTotalAttrs(131L);
        Map<String, Map<Boolean, Long>> propSummary1 = new HashMap<>();
        attrConfig1.setPropSummary(propSummary1);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails1 = new HashMap<>();
            propDetails1.put(Boolean.FALSE, 46L);
            propDetails1.put(Boolean.TRUE, 85L);
            propSummary1.put(propertyName, propDetails1);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig2 = new AttrConfigCategoryOverview<>();
        result.put(Category.GROWTH_TRENDS.getName(), attrConfig2);
        attrConfig2.setLimit(500L);
        attrConfig2.setTotalAttrs(9L);
        Map<String, Map<Boolean, Long>> propSummary2 = new HashMap<>();
        attrConfig2.setPropSummary(propSummary2);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails2 = new HashMap<>();
            propDetails2.put(Boolean.FALSE, 9L);
            propSummary2.put(propertyName, propDetails2);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig3 = new AttrConfigCategoryOverview<>();
        result.put(Category.INTENT.getName(), attrConfig3);
        attrConfig3.setLimit(500L);
        attrConfig3.setTotalAttrs(10960L);
        Map<String, Map<Boolean, Long>> propSummary3 = new HashMap<>();
        attrConfig3.setPropSummary(propSummary3);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails3 = new HashMap<>();
            propDetails3.put(Boolean.FALSE, 7368L);
            propDetails3.put(Boolean.TRUE, 3592L);
            propSummary3.put(propertyName, propDetails3);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig4 = new AttrConfigCategoryOverview<>();
        result.put(Category.LEAD_INFORMATION.getName(), attrConfig4);
        attrConfig4.setLimit(500L);
        attrConfig4.setTotalAttrs(1L);
        Map<String, Map<Boolean, Long>> propSummary4 = new HashMap<>();
        attrConfig4.setPropSummary(propSummary4);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails4 = new HashMap<>();
            propDetails4.put(Boolean.FALSE, 1L);
            propSummary4.put(propertyName, propDetails4);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig5 = new AttrConfigCategoryOverview<>();
        result.put(Category.ACCOUNT_INFORMATION.getName(), attrConfig5);
        attrConfig5.setLimit(500L);
        attrConfig5.setTotalAttrs(1L);
        Map<String, Map<Boolean, Long>> propSummary5 = new HashMap<>();
        attrConfig5.setPropSummary(propSummary5);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails5 = new HashMap<>();
            propDetails5.put(Boolean.FALSE, 1L);
            propSummary5.put(propertyName, propDetails5);
        }

        AttrConfigCategoryOverview<Boolean> attrConfig6 = new AttrConfigCategoryOverview<>();
        result.put(Category.ONLINE_PRESENCE.getName(), attrConfig6);
        attrConfig6.setLimit(500L);
        attrConfig6.setTotalAttrs(0L);
        Map<String, Map<Boolean, Long>> propSummary6 = new HashMap<>();
        attrConfig6.setPropSummary(propSummary6);
        for (String propertyName : propertyNames) {
            Map<Boolean, Long> propDetails6 = new HashMap<>();
            propSummary6.put(propertyName, propDetails6);
        }
        return result;
    }

    private List<AttrConfigOverview<?>> generatePropertyAttrConfigOverview(String propertyName) {
        List<AttrConfigOverview<?>> result = new ArrayList<>();
        AttrConfigOverview<Boolean> attrConfig1 = new AttrConfigOverview<>();
        attrConfig1.setCategory(Category.FIRMOGRAPHICS);
        attrConfig1.setLimit(500L);
        attrConfig1.setTotalAttrs(131L);
        Map<String, Map<Boolean, Long>> propSummary1 = new HashMap<>();
        attrConfig1.setPropSummary(propSummary1);
        Map<Boolean, Long> propDetails1 = new HashMap<>();
        propDetails1.put(Boolean.FALSE, 46L);
        propDetails1.put(Boolean.TRUE, 85L);
        propSummary1.put(propertyName, propDetails1);
        result.add(attrConfig1);

        AttrConfigOverview<Boolean> attrConfig2 = new AttrConfigOverview<>();
        attrConfig2.setCategory(Category.GROWTH_TRENDS);
        attrConfig2.setLimit(500L);
        attrConfig2.setTotalAttrs(9L);
        Map<String, Map<Boolean, Long>> propSummary2 = new HashMap<>();
        attrConfig2.setPropSummary(propSummary2);
        Map<Boolean, Long> propDetails2 = new HashMap<>();
        propDetails2.put(Boolean.FALSE, 9L);
        propSummary2.put(propertyName, propDetails2);
        result.add(attrConfig2);

        AttrConfigOverview<Boolean> attrConfig3 = new AttrConfigOverview<>();
        attrConfig3.setCategory(Category.INTENT);
        attrConfig3.setLimit(500L);
        attrConfig3.setTotalAttrs(10960L);
        Map<String, Map<Boolean, Long>> propSummary3 = new HashMap<>();
        attrConfig3.setPropSummary(propSummary3);
        Map<Boolean, Long> propDetails3 = new HashMap<>();
        propDetails3.put(Boolean.FALSE, 7368L);
        propDetails3.put(Boolean.TRUE, 3592L);
        propSummary3.put(propertyName, propDetails3);
        result.add(attrConfig3);

        AttrConfigOverview<Boolean> attrConfig4 = new AttrConfigOverview<>();
        attrConfig4.setCategory(Category.LEAD_INFORMATION);
        attrConfig4.setLimit(500L);
        attrConfig4.setTotalAttrs(1L);
        Map<String, Map<Boolean, Long>> propSummary4 = new HashMap<>();
        attrConfig4.setPropSummary(propSummary4);
        Map<Boolean, Long> propDetails4 = new HashMap<>();
        propDetails4.put(Boolean.FALSE, 1L);
        propSummary4.put(propertyName, propDetails4);
        result.add(attrConfig4);

        AttrConfigOverview<Boolean> attrConfig5 = new AttrConfigOverview<>();
        attrConfig5.setCategory(Category.ACCOUNT_INFORMATION);
        attrConfig5.setLimit(500L);
        attrConfig5.setTotalAttrs(1L);
        Map<String, Map<Boolean, Long>> propSummary5 = new HashMap<>();
        attrConfig5.setPropSummary(propSummary5);
        Map<Boolean, Long> propDetails5 = new HashMap<>();
        propDetails5.put(Boolean.FALSE, 1L);
        propSummary5.put(propertyName, propDetails5);
        result.add(attrConfig5);

        AttrConfigOverview<Boolean> attrConfig6 = new AttrConfigOverview<>();
        attrConfig6.setCategory(Category.ONLINE_PRESENCE);
        attrConfig6.setLimit(500L);
        attrConfig6.setTotalAttrs(0L);
        Map<String, Map<Boolean, Long>> propSummary6 = new HashMap<>();
        attrConfig6.setPropSummary(propSummary6);
        Map<Boolean, Long> propDetails6 = new HashMap<>();
        propSummary6.put(propertyName, propDetails6);
        result.add(attrConfig6);
        return result;
    }

}
