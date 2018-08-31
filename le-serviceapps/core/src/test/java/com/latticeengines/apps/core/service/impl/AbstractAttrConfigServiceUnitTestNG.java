package com.latticeengines.apps.core.service.impl;

import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

public class AbstractAttrConfigServiceUnitTestNG {

    @InjectMocks
    private AbstractAttrConfigService cdlAttrConfigServiceImpl = new AttrConfigServiceTestImpl();

    private static final String displayName1 = "displayName";
    private static final String displayName2 = "displayName2";
    private static Tenant tenant;
    private static int intentLimit = 20;
    private static int technologyLimit = 32;
    private static final Logger log = LoggerFactory.getLogger(AbstractAttrConfigServiceUnitTestNG.class);
    @Mock
    private ActivationLimitValidator limitationValidator;

    @BeforeTest(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        tenant = new Tenant("tenantId");
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);
        doReturn(intentLimit).when(limitationValidator).getMaxPremiumLeadEnrichmentAttributesByLicense(tenant.getId(),
                DataLicense.BOMBORA.getDataLicense());
        MultiTenantContext.setTenant(tenant);
        doReturn(technologyLimit).when(limitationValidator)
                .getMaxPremiumLeadEnrichmentAttributesByLicense(tenant.getId(), DataLicense.HG.getDataLicense());
    }

    @Test(groups = "unit")
    public void testGetActualValue() {
        Object actualValue = cdlAttrConfigServiceImpl
                .getActualValue(generateDisplayNamePropertyAllowedForCustomizationWithNoCustomValue());
        Assert.assertEquals(actualValue, displayName1);
        actualValue = cdlAttrConfigServiceImpl.getActualValue(generateDisplayNamePropertyDisallowedForCustomization());
        Assert.assertEquals(actualValue, displayName1);
        actualValue = cdlAttrConfigServiceImpl
                .getActualValue(generateDisplayNamePropertyAllowedForCustomizationWithCustomValue());
        Assert.assertEquals(actualValue, displayName2);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "unit")
    public void testGetAttrConfigOverviewWithSomeActiveAttrs() {
        AttrConfigCategoryOverview overview = cdlAttrConfigServiceImpl.getAttrConfigOverview(
                generatePropertyListWithSomeActive(), Category.INTENT, Arrays.asList(ColumnMetadataKey.State), false);
        log.info("overviewWithSomeActive is " + overview);
        // attr9 's State allowCustomization is false. For Activate/Deactivate
        // page, hide attributes that are: Inactive and AllowCustomization=FALSE
        Assert.assertEquals(overview.getTotalAttrs() - (generatePropertyListWithSomeActive().size() - 3), 0);
        Assert.assertEquals(overview.getLimit() - intentLimit, 0);
        Map<String, Map<?, Long>> propSummary = overview.getPropSummary();
        Assert.assertNotNull(propSummary);
        Assert.assertEquals(propSummary.size(), 1);
        Assert.assertTrue(propSummary.containsKey(ColumnMetadataKey.State));
        Map<?, Long> map = propSummary.get(ColumnMetadataKey.State);

        Assert.assertEquals(map.get(AttrState.Inactive).longValue() - 3, 0L);
        Assert.assertEquals(map.get(AttrState.Active).longValue() - 3, 0L);

        overview = cdlAttrConfigServiceImpl.getAttrConfigOverview(generatePropertyListWithSomeUsedForSegment(),
                Category.FIRMOGRAPHICS, getPropertyNames(), true);
        log.info("overviewWithWithSomeUsedForSegment is " + overview);
        Assert.assertEquals(overview.getTotalAttrs() - 6, 0);
        Assert.assertNull(overview.getLimit());
        propSummary = overview.getPropSummary();
        Assert.assertNotNull(propSummary);
        Assert.assertEquals(propSummary.size(), getPropertyNames().size());
        Assert.assertTrue(propSummary.containsKey(ColumnSelection.Predefined.Segment.getName()));
        Assert.assertTrue(propSummary.containsKey(ColumnSelection.Predefined.Enrichment.getName()));
        Assert.assertTrue(propSummary.containsKey(ColumnSelection.Predefined.TalkingPoint.getName()));
        Assert.assertTrue(propSummary.containsKey(ColumnSelection.Predefined.CompanyProfile.getName()));
        map = propSummary.get(ColumnSelection.Predefined.Segment.getName());
        Assert.assertEquals(map.get(Boolean.TRUE).longValue() - 2, 0L);
        // For Enable/Disable page, hide hide attributes that are: disabled and
        // AllowCustomization=FALSE.
        Assert.assertEquals(map.get(Boolean.FALSE).longValue() - 3, 0L);

        map = propSummary.get(ColumnSelection.Predefined.Enrichment.getName());
        Assert.assertEquals(map.get(Boolean.TRUE).longValue() - 6, 0L);
        map = propSummary.get(ColumnSelection.Predefined.TalkingPoint.getName());
        Assert.assertEquals(map.get(Boolean.TRUE).longValue() - 6, 0L);
        map = propSummary.get(ColumnSelection.Predefined.CompanyProfile.getName());
        Assert.assertEquals(map.get(Boolean.TRUE).longValue() - 6, 0L);

        overview = cdlAttrConfigServiceImpl.getAttrConfigOverview(
                AttrConfigTestUtils.generatePropertyList(Category.FIRMOGRAPHICS, false, false, false, false, false),
                Category.INTENT, getPropertyNames(), true);
        log.info("generatePropertyListWithAllInactive is " + overview);
        Assert.assertEquals(overview.getTotalAttrs(), new Long(0));
    }

    @Test(groups = "unit")
    public void testRenderMethodWithCornerCase() {
        try {
            cdlAttrConfigServiceImpl.render(null, null);
        } catch (Exception e) {
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_40022);
        }
        AttrConfig config = new AttrConfig();
        config.setAttrName(NamingUtils.timestamp(this.getClass().getSimpleName()));
        try {
            cdlAttrConfigServiceImpl.render(generateMetadataList(Category.FIRMOGRAPHICS),
                    Arrays.asList(config, config));
        } catch (Exception e) {
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_40023);
        }
        // system don't render internal attributes
        List<AttrConfig> renderedConfig = cdlAttrConfigServiceImpl
                .render(Arrays.asList(AttrConfigTestUtils.getAccountIdData(Category.ACCOUNT_ATTRIBUTES)), null);
        Assert.assertEquals(renderedConfig.size(), 0);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "unit")
    public void testRender() {
        // default column metadata don't set flag canSegment, canEnrich,
        ColumnMetadata data = AttrConfigTestUtils.getLDCNonPremiumData(Category.FIRMOGRAPHICS);
        List<ColumnMetadata> dataList = Arrays.asList(data);
        List<AttrConfig> renderList = cdlAttrConfigServiceImpl.render(dataList, null);
        Assert.assertEquals(renderList.size(), dataList.size());
        AttrConfig config = renderList.get(0);
        Map<String, AttrConfigProp<?>> props = config.getAttrProps();
        // currently, always render 11 entries
        Assert.assertEquals(props.size(), 11);

        AttrConfigProp<String> segmentProp = config.getProperty(ColumnSelection.Predefined.Segment.name());
        AttrConfigProp<String> companyProfileProp = config
                .getProperty(ColumnSelection.Predefined.CompanyProfile.name());
        AttrConfigProp<String> talkingPointProp = config.getProperty(ColumnSelection.Predefined.TalkingPoint.name());
        AttrConfigProp<String> enrichmentProp = config.getProperty(ColumnSelection.Predefined.Enrichment.name());
        Assert.assertEquals(segmentProp.isAllowCustomization(), Boolean.FALSE);
        Assert.assertEquals(companyProfileProp.isAllowCustomization(), Boolean.FALSE);
        Assert.assertEquals(talkingPointProp.isAllowCustomization(), Boolean.FALSE);
        Assert.assertEquals(enrichmentProp.isAllowCustomization(), Boolean.FALSE);
        data.setCanEnrich(true);
        data.setCanSegment(true);
        dataList = Arrays.asList(data);
        // transfer null customer config
        renderList = cdlAttrConfigServiceImpl.render(dataList, null);
        Assert.assertEquals(renderList.size(), dataList.size());
        List<AttrConfig> expectedList = Arrays
                .asList(AttrConfigTestUtils.getLDCNonPremiumAttr(Category.FIRMOGRAPHICS, false));
        log.info("renderList: ");
        for (AttrConfig x : renderList) {
            log.info(JsonUtils.serialize(x));
        }
        log.info("expectedList: ");
        for (AttrConfig x : expectedList) {
            log.info(JsonUtils.serialize(x));
        }
        Assert.assertEquals(renderList, expectedList);
        // transfer empty customer config
        renderList = cdlAttrConfigServiceImpl.render(dataList, new ArrayList<>());
        Assert.assertEquals(renderList.size(), dataList.size());
        Assert.assertEquals(renderList, expectedList);
        // transfer custom config with partial config
        config = new AttrConfig();
        config.setAttrName("LDC Non-Premium");
        renderList = cdlAttrConfigServiceImpl.render(dataList, Arrays.asList(config));
        Assert.assertEquals(expectedList, renderList);
        // transfer customer config
        renderList = cdlAttrConfigServiceImpl.render(dataList, expectedList);
        Assert.assertEquals(expectedList, renderList);
    }

    @Test(groups = "unit")
    public void testRenderAndTrim() {
        // transfer one list of metadata, after two time's render and trim,
        // verify it same at two different times
        List<ColumnMetadata> metadataList = generateMetadataList(Category.FIRMOGRAPHICS);
        List<AttrConfig> customConfig = new ArrayList<>();
        List<AttrConfig> renderConfig = cdlAttrConfigServiceImpl.render(metadataList, customConfig);
        List<AttrConfig> copiedList = new ArrayList<>();
        renderConfig.forEach(e -> copiedList.add(e.clone()));

        List<AttrConfig> trimConfig = cdlAttrConfigServiceImpl.trim(renderConfig);
        List<AttrConfig> renderConfig2 = cdlAttrConfigServiceImpl.render(metadataList, trimConfig);

        Assert.assertEquals(renderConfig.size(), renderConfig2.size());
        Assert.assertEquals(copiedList, renderConfig2);

        List<AttrConfig> trimConfig2 = cdlAttrConfigServiceImpl.trim(renderConfig2);
        Assert.assertEquals(trimConfig.size(), trimConfig2.size());
        Assert.assertEquals(trimConfig, trimConfig2);
    }

    private List<String> getPropertyNames() {
        return Arrays.asList(ColumnSelection.Predefined.Segment.getName(),
                ColumnSelection.Predefined.Enrichment.getName(), ColumnSelection.Predefined.TalkingPoint.getName(),
                ColumnSelection.Predefined.CompanyProfile.getName());
    }

    private AttrConfigProp<String> generateDisplayNamePropertyAllowedForCustomizationWithNoCustomValue() {
        AttrConfigProp<String> displayNameProp = new AttrConfigProp<String>();
        displayNameProp.setSystemValue(displayName1);
        displayNameProp.setAllowCustomization(true);
        return displayNameProp;
    }

    private AttrConfigProp<String> generateDisplayNamePropertyDisallowedForCustomization() {
        AttrConfigProp<String> displayNameProp = new AttrConfigProp<String>();
        displayNameProp.setSystemValue(displayName1);
        displayNameProp.setAllowCustomization(false);
        return displayNameProp;
    }

    private AttrConfigProp<String> generateDisplayNamePropertyAllowedForCustomizationWithCustomValue() {
        AttrConfigProp<String> displayNameProp = new AttrConfigProp<String>();
        displayNameProp.setSystemValue(displayName1);
        displayNameProp.setAllowCustomization(true);
        displayNameProp.setCustomValue(displayName2);
        return displayNameProp;
    }

    private List<AttrConfig> generatePropertyListWithSomeActive() {
        List<AttrConfig> renderedList = Arrays.asList(
                AttrConfigTestUtils.getLDCNonPremiumAttr(Category.FIRMOGRAPHICS, true),
                AttrConfigTestUtils.getLDCPremiumAttr(Category.FIRMOGRAPHICS, true), //
                AttrConfigTestUtils.getLDCInternalAttr(Category.FIRMOGRAPHICS, true), //
                AttrConfigTestUtils.getCDLStdAttr(Category.FIRMOGRAPHICS, true), //
                AttrConfigTestUtils.getCDLLookIDAttr(Category.FIRMOGRAPHICS, false), //
                AttrConfigTestUtils.getCDLAccountExtensionAttr(Category.FIRMOGRAPHICS, false), //
                AttrConfigTestUtils.getCDLContactExtensionAttr(Category.FIRMOGRAPHICS, false), //
                AttrConfigTestUtils.getCDLDerivedPBAttr(Category.FIRMOGRAPHICS, false), //
                AttrConfigTestUtils.getCDLRatingAttr(Category.FIRMOGRAPHICS, false));
        return renderedList;
    }

    private List<AttrConfig> generatePropertyListWithSomeUsedForSegment() {
        List<AttrConfig> renderedList = Arrays.asList(
                AttrConfigTestUtils.getLDCNonPremiumAttr(Category.INTENT, true, true, false, false, false),
                AttrConfigTestUtils.getLDCPremiumAttr(Category.INTENT, true, true, false, false, false), //
                AttrConfigTestUtils.getLDCInternalAttr(Category.INTENT, true, true, false, false, false), //
                AttrConfigTestUtils.getCDLStdAttr(Category.INTENT, true, false, false, false, false), //
                AttrConfigTestUtils.getCDLLookIDAttr(Category.INTENT, false, true, false, false, false), //
                AttrConfigTestUtils.getCDLAccountExtensionAttr(Category.INTENT, true, false, false, false, false), //
                AttrConfigTestUtils.getCDLContactExtensionAttr(Category.INTENT, true, false, false, false, false), //
                AttrConfigTestUtils.getCDLDerivedPBAttr(Category.INTENT, true, false, false, false, false), //
                AttrConfigTestUtils.getCDLRatingAttr(Category.INTENT, true, false, false, false, false));
        return renderedList;
    }

    private List<ColumnMetadata> generateMetadataList(Category category) {
        List<ColumnMetadata> metadataList = Arrays.asList(AttrConfigTestUtils.getLDCNonPremiumData(category),
                AttrConfigTestUtils.getLDCPremiumData(category), //
                AttrConfigTestUtils.getLDCInternalData(category), //
                AttrConfigTestUtils.getCDLStdData(category), //
                AttrConfigTestUtils.getCDLLookIDData(category), //
                AttrConfigTestUtils.getCDLAccountExtensionData(category), //
                AttrConfigTestUtils.getCDLContactExtensionData(category), //
                AttrConfigTestUtils.getCDLDerivedPBData(category), //
                AttrConfigTestUtils.getCDLRatingData(category));
        return metadataList;
    }

    private List<AttrConfig> generateRenderedList() {
        List<AttrConfig> renderedList = Arrays.asList(AttrConfigTestUtils.getAccountId(),
                AttrConfigTestUtils.getAnnualRevenue(), AttrConfigTestUtils.getCustomeAccountAttr(),
                AttrConfigTestUtils.getContactId(), AttrConfigTestUtils.getContactFirstName());
        return renderedList;
    }

    static class AttrConfigServiceTestImpl extends AbstractAttrConfigService {

        @Override
        protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
            return null;
        }

        @Override
        protected List<ColumnMetadata> getSystemMetadata(Category category) {
            return null;
        }

    }

}
