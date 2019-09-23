package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.app.exposed.service.CommonTenantConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.AttrConfigSelection;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.AttrDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.SubcategoryDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

@SuppressWarnings("deprecation")
public class AttrConfigServiceImplUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImplUnitTestNG.class);

    @Mock
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Mock
    private UserService userService;

    @Mock
    private ActionService actionService;

    @Mock
    private CommonTenantConfigService appTenantConfigService;

    @InjectMocks
    private AttrConfigServiceImpl attrConfigService;

    private static Tenant tenant;

    private static final String usage = "Export";

    @BeforeTest(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        tenant = new Tenant("tenantId");
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "unit")
    public void testGetOverallAttrConfigActivationOverview() {
        when(cdlAttrConfigProxy.getAttrConfigOverview(anyString(), Matchers.anyList(), Matchers.anyList(),
                anyBoolean())).thenReturn(
                        AttrConfigServiceImplTestUtils.generatePremiumCategoryAttrConfigActivationOverview());
        AttrConfigStateOverview overview = attrConfigService.getOverallAttrConfigActivationOverview();
        List<AttrConfigSelection> result = overview.getSelections();
        Assert.assertEquals(result.size(), Category.getPremiumCategories().size());

        AttrConfigSelection categoryOverview = result.get(0);
        Assert.assertEquals(categoryOverview.getTotalAttrs(), AttrConfigServiceImplTestUtils.totalIntentAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), AttrConfigServiceImplTestUtils.intentLimit);
        Assert.assertEquals(categoryOverview.getSelected(), AttrConfigServiceImplTestUtils.activeForIntent);
        Assert.assertEquals(categoryOverview.getDisplayName(), Category.INTENT.getName());

        categoryOverview = result.get(1);
        Assert.assertEquals(categoryOverview.getTotalAttrs(), AttrConfigServiceImplTestUtils.totalTpAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), AttrConfigServiceImplTestUtils.tpLimit);
        Assert.assertEquals(categoryOverview.getSelected(), AttrConfigServiceImplTestUtils.activeForTp);
        Assert.assertEquals(categoryOverview.getDisplayName(), Category.TECHNOLOGY_PROFILE.getName());

        categoryOverview = result.get(2);
        Assert.assertEquals(categoryOverview.getTotalAttrs(), AttrConfigServiceImplTestUtils.totalWebsiteKeywordAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), AttrConfigServiceImplTestUtils.websiteKeywordLimit);
        Assert.assertEquals(categoryOverview.getSelected(), AttrConfigServiceImplTestUtils.activeForWebsiteKeyword);
        Assert.assertEquals(categoryOverview.getDisplayName(), Category.WEBSITE_KEYWORDS.getName());

        categoryOverview = result.get(3);
        Assert.assertEquals(categoryOverview.getTotalAttrs(), AttrConfigServiceImplTestUtils.totalAccountAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), AttrConfigServiceImplTestUtils.accountLimit);
        Assert.assertEquals(categoryOverview.getSelected(), AttrConfigServiceImplTestUtils.activeForAccount);
        Assert.assertEquals(categoryOverview.getDisplayName(), Category.ACCOUNT_ATTRIBUTES.getName());

        categoryOverview = result.get(4);
        Assert.assertEquals(categoryOverview.getTotalAttrs(), AttrConfigServiceImplTestUtils.totalContactAttrs);
        Assert.assertEquals(categoryOverview.getLimit(), AttrConfigServiceImplTestUtils.contactLimit);
        Assert.assertEquals(categoryOverview.getSelected(), AttrConfigServiceImplTestUtils.activeForContact);
        Assert.assertEquals(categoryOverview.getDisplayName(), Category.CONTACT_ATTRIBUTES.getName());

    }

    @Test(groups = "unit", dependsOnMethods = { "testGetOverallAttrConfigActivationOverview" })
    public void testGetOverallAttrConfigUsageOverview() {
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                Arrays.asList(ColumnSelection.Predefined.usageProperties), true))
                        .thenReturn(AttrConfigServiceImplTestUtils.generatePropertyAttrConfigOverviewForUsage(
                                Arrays.asList(ColumnSelection.Predefined.usageProperties)));
        when(appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(anyString(), anyString()))
                .thenReturn(1000);
        AttrConfigUsageOverview usageOverview = attrConfigService.getOverallAttrConfigUsageOverview();
        log.info("overall usageOverview is " + usageOverview);
        List<AttrConfigSelection> selections = usageOverview.getSelections();
        Assert.assertEquals(selections.size(), ColumnSelection.Predefined.usageProperties.length);
        for (int i = 0; i < ColumnSelection.Predefined.usageProperties.length; i++) {
            Assert.assertEquals(selections.get(i).getDisplayName(),
                    AttrConfigServiceImpl.mapUsageToDisplayName(ColumnSelection.Predefined.usageProperties[i]));
            Assert.assertEquals(selections.get(i).getSelected() - 3677, 0);
            Assert.assertEquals(selections.get(i).getCategories().size(), 6);
            Assert.assertNotNull(selections.get(i).getCategories().values());
        }
        // Export
        Assert.assertNotNull(selections.get(1).getLimit());
        // Company Profile
        Assert.assertNotNull(selections.get(4).getLimit());
    }

    @Test(groups = "unit")
    public void testGetOverallAttrConfigNameOverview() {
        when(cdlAttrConfigProxy.getAttrConfigOverview(anyString(), Matchers.anyList(), Matchers.anyList(),
                anyBoolean())).thenReturn(
                        AttrConfigServiceImplTestUtils.generateAccountAndContactCategoryAttrConfigNameOverview());
        AttrConfigStateOverview overview = attrConfigService.getOverallAttrConfigNameOverview();
        List<AttrConfigSelection> result = overview.getSelections();
        Assert.assertEquals(result.size(), 2);

        AttrConfigSelection categoryOverview = result.get(0);
        Assert.assertEquals(categoryOverview.getTotalAttrs(), AttrConfigServiceImplTestUtils.activeForAccount);
        Assert.assertEquals(categoryOverview.getDisplayName(), Category.ACCOUNT_ATTRIBUTES.getName());

        categoryOverview = result.get(1);
        Assert.assertEquals(categoryOverview.getTotalAttrs(), AttrConfigServiceImplTestUtils.activeForContact);
        Assert.assertEquals(categoryOverview.getDisplayName(), Category.CONTACT_ATTRIBUTES.getName());

    }

    @Test(groups = "unit")
    public void testGenerateAttrConfigRequestForUsage() {
        AttrConfigSelectionRequest request = new AttrConfigSelectionRequest();
        request.setDeselect(Arrays.asList(AttrConfigServiceImplTestUtils.deselect));
        request.setSelect(Arrays.asList(AttrConfigServiceImplTestUtils.select));
        AttrConfigRequest attrConfigRequest = attrConfigService.generateAttrConfigRequestForUsage(
                Category.INTENT.getName(), AttrConfigServiceImpl.mapDisplayNameToUsage(usage), request);
        List<AttrConfig> attrConfigs = attrConfigRequest.getAttrConfigs();
        Assert.assertEquals(attrConfigs.size(),
                AttrConfigServiceImplTestUtils.select.length + AttrConfigServiceImplTestUtils.deselect.length);
        for (AttrConfig attrConfig : attrConfigs) {
            log.info("attrConfig is " + JsonUtils.serialize(attrConfig));
            Assert.assertNotNull(attrConfig.getAttrName());
            Assert.assertEquals(attrConfig.getEntity(), BusinessEntity.Account);
            Assert.assertTrue(attrConfig.getAttrProps().containsKey(ColumnSelection.Predefined.Enrichment.getName()));
        }
    }

    @Test(groups = "unit")
    public void testUpdateAttrConfigsForUsage() {
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigService.updateAttrConfigsForUsage(Category.PRODUCT_SPEND, attrConfigs,
                "Product_02622F2BC93FF8CBE5BAF4A29239C543_Revenue", ColumnSelection.Predefined.Model.getName(), true);
        Assert.assertEquals(attrConfigs.get(0).getEntity(), BusinessEntity.AnalyticPurchaseState);
        attrConfigService.updateAttrConfigsForUsage(Category.PRODUCT_SPEND, attrConfigs, "Product_Puchase",
                ColumnSelection.Predefined.Enrichment.getName(), true);
        Assert.assertEquals(attrConfigs.get(1).getEntity(), BusinessEntity.PurchaseHistory);
    }

    @Test(groups = "unit")
    public void testGenerateAttrConfigRequestForActivation() {
        doReturn(AccessLevel.SUPER_ADMIN).when(userService).getAccessLevel(anyString(), nullable(String.class));
        AttrConfigSelectionRequest request = new AttrConfigSelectionRequest();
        request.setDeselect(Arrays.asList(AttrConfigServiceImplTestUtils.deselect));
        request.setSelect(Arrays.asList(AttrConfigServiceImplTestUtils.select));
        AttrConfigRequest attrConfigRequest = attrConfigService
                .generateAttrConfigRequestForActivation(Category.CONTACT_ATTRIBUTES.getName(), request);
        List<AttrConfig> attrConfigs = attrConfigRequest.getAttrConfigs();
        Assert.assertEquals(attrConfigs.size(),
                AttrConfigServiceImplTestUtils.select.length + AttrConfigServiceImplTestUtils.deselect.length);
        for (AttrConfig attrConfig : attrConfigs) {
            Assert.assertNotNull(attrConfig.getAttrName());
            Assert.assertEquals(attrConfig.getEntity(), BusinessEntity.Contact);
            Assert.assertTrue(attrConfig.getAttrProps().containsKey(ColumnMetadataKey.State));
            log.info("attrConfig is " + JsonUtils.serialize(attrConfig));
        }

        doReturn(AccessLevel.INTERNAL_USER).when(userService).getAccessLevel(anyString(), nullable(String.class));
        try {
            attrConfigRequest = attrConfigService
                    .generateAttrConfigRequestForActivation(Category.CONTACT_ATTRIBUTES.getName(), request);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LedpException);
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18185);
        }

        // test external admin can activate attributes
        doReturn(AccessLevel.EXTERNAL_ADMIN).when(userService).getAccessLevel(anyString(), nullable(String.class));
        try {
            request.setDeselect(new ArrayList<>());
            attrConfigRequest = attrConfigService
                    .generateAttrConfigRequestForActivation(Category.CONTACT_ATTRIBUTES.getName(), request);
        } catch (Exception e) {
            Assert.fail("Should not throw any exception");
        }
    }

    @Test(groups = "unit")
    public void testGenerateAttrConfigRequestForName() {
        AttrDetail attr1Change = new AttrDetail();
        attr1Change.setAttribute("attr1");
        attr1Change.setDisplayName("att1-name-update");
        AttrDetail attr2Change = new AttrDetail();
        attr2Change.setAttribute("attr2");
        attr2Change.setDisplayName("att2-name-update");
        attr2Change.setDescription("att2-description-update");
        SubcategoryDetail request = new SubcategoryDetail();
        request.setAttributes(Arrays.asList(attr1Change, attr2Change));
        AttrConfigRequest attrConfigRequest = attrConfigService
                .generateAttrConfigRequestForName(Category.CONTACT_ATTRIBUTES.getName(), request);
        List<AttrConfig> attrConfigs = attrConfigRequest.getAttrConfigs();
        Assert.assertEquals(attrConfigs.size(), 2);
        for (AttrConfig attrConfig : attrConfigs) {
            log.info("attrConfig is " + JsonUtils.serialize(attrConfig));
            if (attrConfig.getAttrName().equals("attr1")) {
                Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.DisplayName).getCustomValue(),
                        "att1-name-update");
            } else if (attrConfig.getAttrName().equals("attr2")) {
                Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.DisplayName).getCustomValue(),
                        "att2-name-update");
                Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.Description).getCustomValue(),
                        "att2-description-update");
            } else {
                Assert.fail("Does not expect the attribute" + attrConfig.getAttrName());
            }
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

    @Test(groups = "unit")
    public void testGetDetailAttrForActivation() {
        AttrConfigRequest request = new AttrConfigRequest();
        request.setAttrConfigs(Arrays.asList(AttrConfigServiceImplTestUtils.getAttr1(Category.CONTACT_ATTRIBUTES, true),
                AttrConfigServiceImplTestUtils.getAttr2(Category.CONTACT_ATTRIBUTES, true), //
                AttrConfigServiceImplTestUtils.getAttr3(Category.CONTACT_ATTRIBUTES, true), //
                AttrConfigServiceImplTestUtils.getAttr4(Category.CONTACT_ATTRIBUTES, true), //
                AttrConfigServiceImplTestUtils.getAttr5(Category.CONTACT_ATTRIBUTES, false), //
                AttrConfigServiceImplTestUtils.getAttr6(Category.CONTACT_ATTRIBUTES, false), //
                AttrConfigServiceImplTestUtils.getAttr7(Category.CONTACT_ATTRIBUTES, false), //
                AttrConfigServiceImplTestUtils.getAttr8(Category.CONTACT_ATTRIBUTES, false), //
                AttrConfigServiceImplTestUtils.getAttr9(Category.CONTACT_ATTRIBUTES, false)));
        when(cdlAttrConfigProxy.getAttrConfigByCategory(tenant.getId(), Category.CONTACT_ATTRIBUTES.getName()))
                .thenReturn(request);
        AttrConfigSelectionDetail activationDetail = attrConfigService
                .getAttrConfigSelectionDetailForState(Category.CONTACT_ATTRIBUTES.getName());
        log.info("testGetDetailAttrForActivation activationDetail is " + activationDetail);
        Assert.assertEquals(activationDetail.getSelected() - 4L, 0);
        Assert.assertEquals(activationDetail.getTotalAttrs() - 8L, 0);
        Assert.assertEquals(activationDetail.getSubcategories().size(), 8);
        Assert.assertTrue(activationDetail.getSubcategories().parallelStream()
                .allMatch(entry -> entry.getHasFrozenAttrs() == false));
    }

    @Test(groups = "unit")
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
        AttrConfigSelectionDetail selectionDetail = attrConfigService
                .getAttrConfigSelectionDetailForUsage(Category.FIRMOGRAPHICS.getName(), "Segmentation");
        log.info("testGetDetailAttrForUsageWithNonPremiumCategory selectionDetail is " + selectionDetail);
        Assert.assertEquals(selectionDetail.getSelected() - 0L, 0);
        Assert.assertEquals(selectionDetail.getTotalAttrs() - 1L, 0);
        Assert.assertEquals(selectionDetail.getSubcategories().size(), 4);
        Assert.assertEquals(selectionDetail.getSubcategories().parallelStream()
                .filter(entry -> entry.getHasFrozenAttrs() == false).count(), 4);
    }

    @Test(groups = "unit")
    public void testGetDetailAttrForUsageOfModeling() {
        AttrConfigRequest request = new AttrConfigRequest();
        // The total number of attribute should not be impacted by the state
        request.setAttrConfigs(
                Arrays.asList(AttrConfigServiceImplTestUtils.getAttr1(Category.TECHNOLOGY_PROFILE, false),
                        AttrConfigServiceImplTestUtils.getAttr2(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr3(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr4(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr5(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr6(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr7(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr8(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr9(Category.TECHNOLOGY_PROFILE, false)));
        when(cdlAttrConfigProxy.getAttrConfigByCategory(tenant.getId(), Category.TECHNOLOGY_PROFILE.getName()))
                .thenReturn(request);
        AttrConfigSelectionDetail selectionDetail = attrConfigService
                .getAttrConfigSelectionDetailForUsage(Category.TECHNOLOGY_PROFILE.getName(), "Modeling");
        log.info("testGetDetailAttrForUsageOfModeling selectionDetail is " + selectionDetail);
        Assert.assertEquals(selectionDetail.getSelected() - 0L, 0);
        Assert.assertEquals(selectionDetail.getTotalAttrs() - 9L, 0);
        Assert.assertEquals(selectionDetail.getSubcategories().size(), 8);
        Assert.assertEquals(selectionDetail.getSubcategories().parallelStream()
                .filter(entry -> entry.getHasFrozenAttrs() == false).count(), 8);
    }

    @Test(groups = "unit")
    public void testGetDetailAttrForUsageOfModelingInCaseOfDeprecation() {
        AttrConfigRequest request = new AttrConfigRequest();
        // The total number of attribute should not be impacted by the state
        request.setAttrConfigs(
                Arrays.asList(AttrConfigServiceImplTestUtils.getAttr1(Category.TECHNOLOGY_PROFILE, false),
                        AttrConfigServiceImplTestUtils.getAttr2(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr3(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr4(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr5(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr6(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr7(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr8(Category.TECHNOLOGY_PROFILE, false), //
                        AttrConfigServiceImplTestUtils.getAttr9(Category.TECHNOLOGY_PROFILE, false)));
        when(cdlAttrConfigProxy.getAttrConfigByCategory(tenant.getId(), Category.TECHNOLOGY_PROFILE.getName()))
                .thenReturn(request);
        // the first AttrConfig is set to be deprecated
        request.getAttrConfigs().get(0).setShouldDeprecate(true);
        AttrConfigSelectionDetail selectionDetail = attrConfigService
                .getAttrConfigSelectionDetailForUsage(Category.TECHNOLOGY_PROFILE.getName(), "Modeling");
        log.info("testGetDetailAttrForUsageOfModelingInCaseOfDeprecation selectionDetail is " + selectionDetail);
        Assert.assertEquals(selectionDetail.getSelected() - 0L, 0);
        Assert.assertEquals(selectionDetail.getTotalAttrs() - 8L, 0);
        Assert.assertEquals(selectionDetail.getSubcategories().size(), 8);
        Assert.assertEquals(selectionDetail.getSubcategories().parallelStream()
                .filter(entry -> entry.getHasFrozenAttrs() == false).count(), 8);
    }

    @Test(groups = "unit")
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
                .getAttrConfigSelectionDetailForUsage(Category.INTENT.getName(), "Segmentation");
        log.info("testGetDetailAttrForUsageWithPremiumCategory selectionDetail is " + selectionDetail);
        Assert.assertEquals(selectionDetail.getSelected() - 1L, 0);
        Assert.assertEquals(selectionDetail.getTotalAttrs() - 1L, 0);
        Assert.assertEquals(selectionDetail.getSubcategories().size(), 4);
        Assert.assertEquals(selectionDetail.getSubcategories().parallelStream()
                .filter(entry -> entry.getHasFrozenAttrs() == false).count(), 4);
    }

    @Test(groups = "unit")
    public void testGetAttrConfigSelectionDetailForName() {
        AttrConfigRequest request = new AttrConfigRequest();
        request.setAttrConfigs(
                Arrays.asList(AttrConfigServiceImplTestUtils.getAttr1(Category.ACCOUNT_ATTRIBUTES, true, true),
                        AttrConfigServiceImplTestUtils.getAttr2(Category.ACCOUNT_ATTRIBUTES, true, true), //
                        AttrConfigServiceImplTestUtils.getAttr3(Category.ACCOUNT_ATTRIBUTES, true, true), //
                        AttrConfigServiceImplTestUtils.getAttr4(Category.ACCOUNT_ATTRIBUTES, true, false), //
                        AttrConfigServiceImplTestUtils.getAttr5(Category.ACCOUNT_ATTRIBUTES, false, true), //
                        AttrConfigServiceImplTestUtils.getAttr6(Category.ACCOUNT_ATTRIBUTES, false, false), //
                        AttrConfigServiceImplTestUtils.getAttr7(Category.ACCOUNT_ATTRIBUTES, false, false), //
                        AttrConfigServiceImplTestUtils.getAttr8(Category.ACCOUNT_ATTRIBUTES, false, false), //
                        AttrConfigServiceImplTestUtils.getAttr9(Category.ACCOUNT_ATTRIBUTES, false, false)));
        when(cdlAttrConfigProxy.getAttrConfigByCategory(tenant.getId(), Category.ACCOUNT_ATTRIBUTES.getName()))
                .thenReturn(request);
        List<AttrDetail> selectionDetail = attrConfigService
                .getAttrConfigSelectionDetailForName(Category.ACCOUNT_ATTRIBUTES.getName()).getAttributes();
        log.info("testGetAttrConfigSelectionDetailForName selectionDetail is " + selectionDetail);
        Assert.assertEquals(selectionDetail.size(), 4);
        Assert.assertTrue(selectionDetail.stream().allMatch(detail -> detail.getDefaultName() != null
                && detail.getAttribute() != null && detail.getDescription() != null));
    }

    @Test(groups = "unit")
    public void testUpdateUsageConfig() {

        // happy path
        AttrConfigSelectionRequest request = new AttrConfigSelectionRequest();
        request.setDeselect(Arrays.asList(AttrConfigServiceImplTestUtils.deselect[0]));
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateHappyAttrConfigRequest());
        UIAction uiAction = attrConfigService.updateUsageConfig(Category.INTENT.getName(), "Company Profile", request);
        Assert.assertNotNull(uiAction);
        Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_USAGE_SUCCESS_TITLE);
        Assert.assertEquals(uiAction.getView(), View.Notice);
        Assert.assertEquals(uiAction.getStatus(), Status.Success);

        // attribute level
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateAttrLevelAttrConfigRequest(true));
        try {
            attrConfigService.updateUsageConfig(Category.ACCOUNT_ATTRIBUTES.getName(), "Company Profile", request);
            Assert.fail("Should have thrown exception due to dependency check failure");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UIActionException);
            UIActionException uiActionException = (UIActionException) e;
            uiAction = uiActionException.getUIAction();
            Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_WARNING_ATTRIBUTE_TITLE);
            Assert.assertEquals(uiAction.getView(), View.Modal);
            Assert.assertEquals(uiAction.getStatus(), Status.Error);
            Assert.assertNotNull(uiAction.getMessage());
        }

        // subcategory level
        request.setDeselect(Arrays.asList(AttrConfigServiceImplTestUtils.deselect));
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateSubcategoryLevelAttrConfigRequest(true));
        try {
            attrConfigService.updateUsageConfig(Category.ACCOUNT_ATTRIBUTES.getName(), "Company Profile", request);
            Assert.fail("Should have thrown exception due to dependency check failure");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UIActionException);
            UIActionException uiActionException = (UIActionException) e;
            uiAction = uiActionException.getUIAction();
            Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_WARNING_SUBCATEGORY_TITLE);
            Assert.assertEquals(uiAction.getView(), View.Modal);
            Assert.assertEquals(uiAction.getStatus(), Status.Error);
            Assert.assertNotNull(uiAction.getMessage());
        }
        // category level
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateCategoryLevelAttrConfigRequest(true));
        try {
            attrConfigService.updateUsageConfig(Category.ACCOUNT_ATTRIBUTES.getName(), "Company Profile", request);
            Assert.fail("Should have thrown exception due to dependency check failure");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UIActionException);
            UIActionException uiActionException = (UIActionException) e;
            uiAction = uiActionException.getUIAction();
            Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_WARNING_CATEGORY_TITLE);
            Assert.assertEquals(uiAction.getView(), View.Modal);
            Assert.assertEquals(uiAction.getStatus(), Status.Error);
            Assert.assertNotNull(uiAction.getMessage());
        }
    }

    @Test(groups = "unit")
    public void testUpdateActivationConfig() {
        when(actionService.create(any(Action.class))).thenReturn(new Action());
        doReturn(AccessLevel.SUPER_ADMIN).when(userService).getAccessLevel(anyString(), nullable(String.class));
        // happy path
        AttrConfigSelectionRequest request = new AttrConfigSelectionRequest();
        request.setDeselect(Arrays.asList(AttrConfigServiceImplTestUtils.deselect[0]));
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateHappyAttrConfigRequest());
        UIAction uiAction = attrConfigService.updateActivationConfig(Category.INTENT.getName(), request);
        Assert.assertNotNull(uiAction);
        Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_ACTIVATION_SUCCESS_TITLE);
        Assert.assertEquals(uiAction.getView(), View.Banner);
        Assert.assertEquals(uiAction.getStatus(), Status.Success);
        Assert.assertEquals(uiAction.getMessage(), AttrConfigServiceImpl.UPDATE_ACTIVATION_SUCCESSE_MSG);

        // attribute level
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateAttrLevelAttrConfigRequest(false));
        try {
            attrConfigService.updateActivationConfig(Category.ACCOUNT_ATTRIBUTES.getName(), request);
            Assert.fail("Should have thrown exception due to dependency check failure");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UIActionException);
            UIActionException uiActionException = (UIActionException) e;
            uiAction = uiActionException.getUIAction();
            Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_WARNING_ATTRIBUTE_TITLE);
            Assert.assertEquals(uiAction.getView(), View.Modal);
            Assert.assertEquals(uiAction.getStatus(), Status.Error);
            Assert.assertNotNull(uiAction.getMessage());
        }

        // subcategory level
        request.setDeselect(Arrays.asList(AttrConfigServiceImplTestUtils.deselect));
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateSubcategoryLevelAttrConfigRequest(true));
        try {
            attrConfigService.updateActivationConfig(Category.ACCOUNT_ATTRIBUTES.getName(), request);
            Assert.fail("Should have thrown exception due to dependency check failure");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UIActionException);
            UIActionException uiActionException = (UIActionException) e;
            uiAction = uiActionException.getUIAction();
            Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_WARNING_SUBCATEGORY_TITLE);
            Assert.assertEquals(uiAction.getView(), View.Modal);
            Assert.assertEquals(uiAction.getStatus(), Status.Error);
            Assert.assertNotNull(uiAction.getMessage());
        }
        // category level
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateCategoryLevelAttrConfigRequest(true));
        try {
            attrConfigService.updateUsageConfig(Category.ACCOUNT_ATTRIBUTES.getName(), "Company Profile", request);
            Assert.fail("Should have thrown exception due to dependency check failure");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UIActionException);
            UIActionException uiActionException = (UIActionException) e;
            uiAction = uiActionException.getUIAction();
            Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_WARNING_CATEGORY_TITLE);
            Assert.assertEquals(uiAction.getView(), View.Modal);
            Assert.assertEquals(uiAction.getStatus(), Status.Error);
            Assert.assertNotNull(uiAction.getMessage());
        }

        // validation error
        when(cdlAttrConfigProxy.saveAttrConfig(anyString(), any(AttrConfigRequest.class),
                any(AttrConfigUpdateMode.class)))
                        .thenReturn(AttrConfigServiceImplTestUtils.generateValidationErrorAttrConfigRequest());
        try {
            attrConfigService.updateUsageConfig(Category.ACCOUNT_ATTRIBUTES.getName(), "Company Profile", request);
            Assert.fail("Should have thrown exception due to dependency check failure");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UIActionException);
            UIActionException uiActionException = (UIActionException) e;
            uiAction = uiActionException.getUIAction();
            Assert.assertEquals(uiAction.getTitle(), AttrConfigServiceImpl.UPDATE_FAIL_TITLE);
            Assert.assertEquals(uiAction.getView(), View.Banner);
            Assert.assertEquals(uiAction.getStatus(), Status.Error);
            Assert.assertNotNull(uiAction.getMessage());
            log.info("error message is:" + uiAction.getMessage());
        }
    }

    @Test(groups = "unit")
    public void testGenerateAttrLevelMsg() {
        String html = attrConfigService
                .generateAttrLevelMsg(AttrConfigServiceImplTestUtils.generateAttrLevelAttrConfigRequest(true), true);
        log.info("html for Attr Level is " + html);
        Assert.assertTrue(
                html.contains(String.format("<p>%s</p>", AttrConfigServiceImpl.UPDATE_USAGE_FAIL_ATTRIBUTE_MSG)));
        Assert.assertTrue(html.contains("<b>Model(s):</b><ul><li>re1</li><li>re2</li><li>re3</li></ul>"));
        Assert.assertTrue(html.contains("<b>RatingModel(s):</b><ul><li>rm1</li><li>rm2</li><li>rm3</li></ul>"));
        Assert.assertTrue(html.contains("<b>Segment(s):</b><ul><li>seg1</li><li>seg2</li><li>seg3</li></ul>"));

        html = attrConfigService
                .generateAttrLevelMsg(AttrConfigServiceImplTestUtils.generateAttrLevelAttrConfigRequest(false), false);
        log.info("html for Attr Level is " + html);
        Assert.assertTrue(
                html.contains(String.format("<p>%s</p>", AttrConfigServiceImpl.UPDATE_ACTIVATION_FAIL_ATTRIBUTE_MSG)));
        Assert.assertTrue(html.contains("<li>Segmentation</li>"));
        Assert.assertTrue(html.contains("<li>Company Profile</li>"));
        Assert.assertTrue(html.contains("<li>Export</li>"));
    }

    @Test(groups = "unit")
    public void testUpdateActivationSuccessMsg() {
        String html = attrConfigService.generateUpdateActivationSuccessMsg();
        log.info("html for update Activation is " + html);
    }

    @Test(groups = "unit")
    public void testGenerateSubcategoryLevelMsg() {
        String html = attrConfigService.generateSubcategoryLevelMsg(
                AttrConfigServiceImplTestUtils.generateSubcategoryLevelAttrConfigRequest(true), true);
        log.info("html for Subcategory Level is " + html);
        Assert.assertEquals(html, String.format("<p>%s</p><b>sub1:</b><ul><li>attr4</li><li>attr5</li></ul>",
                AttrConfigServiceImpl.UPDATE_USAGE_FAIL_SUBCATEGORY_MSG));

        html = attrConfigService.generateSubcategoryLevelMsg(
                AttrConfigServiceImplTestUtils.generateSubcategoryLevelAttrConfigRequest(false), false);
        log.info("html for Subcategory Level is " + html);
        Assert.assertEquals(html, String.format("<p>%s</p><b>sub1:</b><ul><li>attr4</li><li>attr5</li></ul>",
                AttrConfigServiceImpl.UPDATE_ACTIVATION_FAIL_SUBCATEGORY_MSG));
    }

    @Test(groups = "unit")
    public void testGenerateCategoryLevelMsg() {
        String html = attrConfigService.generateCategoryLevelMsg(
                AttrConfigServiceImplTestUtils.generateCategoryLevelAttrConfigRequest(true), true);
        log.info("html for Category Level is " + html);
        Assert.assertTrue(
                html.contains(String.format("<p>%s</p>", AttrConfigServiceImpl.UPDATE_USAGE_FAIL_CATEGORY_MSG)));
        Assert.assertTrue(html.contains("<b>sub2:</b><ul><li>attr6</li></ul>"));
        Assert.assertTrue(html.contains("<b>sub1:</b><ul><li>attr4</li><li>attr5</li></ul>"));

        html = attrConfigService.generateCategoryLevelMsg(
                AttrConfigServiceImplTestUtils.generateCategoryLevelAttrConfigRequest(false), false);
        log.info("html for Category Level is " + html);
        Assert.assertTrue(
                html.contains(String.format("<p>%s</p>", AttrConfigServiceImpl.UPDATE_ACTIVATION_FAIL_CATEGORY_MSG)));
        Assert.assertTrue(html.contains("<b>sub2:</b><ul><li>attr6</li></ul>"));
        Assert.assertTrue(html.contains("<b>sub1:</b><ul><li>attr4</li><li>attr5</li></ul>"));
    }

    @Test(groups = "unit")
    public void testGenerateErrorMsg() {
        AttrConfigRequest request = AttrConfigServiceImplTestUtils.generateValidationErrorAttrConfigRequest();
        String html = attrConfigService.generateErrorMsg(request);
        log.info("html for Error is: " + html);
        Assert.assertTrue(html.contains(String.format("<p>%s</p>", AttrConfigServiceImpl.UPDATE_FAIL_MSG)));

        Assert.assertTrue(html.contains("<b>Cannot activate deprecated attribute: 2 attributes:</b>"));
        Assert.assertTrue(html.contains("<li>User cannot change deprecated attribute to active.</li>"));


    }
}
