package com.latticeengines.apps.core.service.impl;

import static org.mockito.Mockito.doReturn;

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

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
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
    private LimitationValidator limitationValidator;

    @BeforeTest(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        tenant = new Tenant("tenantId");
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);
        doReturn(intentLimit).when(limitationValidator).getMaxPremiumLeadEnrichmentAttributesByLicense(tenant.getId(),
                DataLicense.BOMBORA);
        MultiTenantContext.setTenant(tenant);
        doReturn(technologyLimit).when(limitationValidator)
                .getMaxPremiumLeadEnrichmentAttributesByLicense(tenant.getId(), DataLicense.HG);
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
    public void testGetAttrConfigOverview() {
        AttrConfigOverview overview = cdlAttrConfigServiceImpl.getAttrConfigOverview(generateRenderedList(),
                Category.INTENT, ColumnMetadataKey.State);
        Assert.assertEquals(overview.getCategory(), Category.INTENT);
        Assert.assertEquals((overview.getTotalAttrs() - generateRenderedList().size()), 0L);
        Assert.assertNotNull(overview.getLimit());
        Map<String, Map<?, Long>> propSummary = overview.getPropSummary();
        Assert.assertNotNull(propSummary);

        Assert.assertTrue(propSummary.containsKey(ColumnMetadataKey.State));
        Map<?, Long> map = propSummary.get(ColumnMetadataKey.State);
        Assert.assertEquals(map.get(AttrState.Inactive).longValue() - 1, 0L);
        Assert.assertEquals(map.get(AttrState.Active).longValue() - 4, 0L);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "unit")
    public void testGetAttrConfigOverviewWithFourActiveAttrs() {
        AttrConfigCategoryOverview overview = cdlAttrConfigServiceImpl.getAttrConfigOverview(
                generatePropertyListWithSomeActive(), Category.INTENT, Arrays.asList(ColumnMetadataKey.State), false);
        log.info("overviewWithSomeActive is " + overview);
        Assert.assertEquals(overview.getTotalAttrs() - generatePropertyListWithSomeActive().size(), 0);
        Assert.assertEquals(overview.getLimit() - intentLimit, 0);
        Map<String, Map<?, Long>> propSummary = overview.getPropSummary();
        Assert.assertNotNull(propSummary);
        Assert.assertEquals(propSummary.size(), 1);
        Assert.assertTrue(propSummary.containsKey(ColumnMetadataKey.State));
        Map<?, Long> map = propSummary.get(ColumnMetadataKey.State);
        Assert.assertEquals(map.get(AttrState.Inactive).longValue() - 5, 0L);
        Assert.assertEquals(map.get(AttrState.Active).longValue() - 4, 0L);

        overview = cdlAttrConfigServiceImpl.getAttrConfigOverview(generatePropertyListWithSomeUsedForSegment(),
                Category.FIRMOGRAPHICS, getPropertyNames(), true);
        log.info("overviewWithWithSomeUsedForSegment is " + overview);
        Assert.assertEquals(overview.getTotalAttrs() - 4, 0);
        Assert.assertNull(overview.getLimit());
        propSummary = overview.getPropSummary();
        Assert.assertNotNull(propSummary);
        Assert.assertEquals(propSummary.size(), getPropertyNames().size());
        Assert.assertTrue(propSummary.containsKey(ColumnSelection.Predefined.Segment.getName()));
        Assert.assertTrue(propSummary.containsKey(ColumnSelection.Predefined.Enrichment.getName()));
        Assert.assertTrue(propSummary.containsKey(ColumnSelection.Predefined.TalkingPoint.getName()));
        Assert.assertTrue(propSummary.containsKey(ColumnSelection.Predefined.CompanyProfile.getName()));
        map = propSummary.get(ColumnSelection.Predefined.Segment.getName());
        Assert.assertEquals(map.get(Boolean.TRUE).longValue() - 1, 0L);
        Assert.assertEquals(map.get(Boolean.FALSE).longValue() - 3, 0L);
        map = propSummary.get(ColumnSelection.Predefined.Enrichment.getName());
        Assert.assertEquals(map.get(Boolean.TRUE).longValue() - 4, 0L);
        map = propSummary.get(ColumnSelection.Predefined.TalkingPoint.getName());
        Assert.assertEquals(map.get(Boolean.TRUE).longValue() - 4, 0L);
        map = propSummary.get(ColumnSelection.Predefined.CompanyProfile.getName());
        Assert.assertEquals(map.get(Boolean.TRUE).longValue() - 4, 0L);
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
        List<AttrConfig> renderedList = Arrays.asList(AttrConfigTestUtils.getAttr1(Category.FIRMOGRAPHICS, true),
                AttrConfigTestUtils.getAttr2(Category.FIRMOGRAPHICS, true), //
                AttrConfigTestUtils.getAttr3(Category.FIRMOGRAPHICS, true), //
                AttrConfigTestUtils.getAttr4(Category.FIRMOGRAPHICS, true), //
                AttrConfigTestUtils.getAttr5(Category.FIRMOGRAPHICS, false), //
                AttrConfigTestUtils.getAttr6(Category.FIRMOGRAPHICS, false), //
                AttrConfigTestUtils.getAttr7(Category.FIRMOGRAPHICS, false), //
                AttrConfigTestUtils.getAttr8(Category.FIRMOGRAPHICS, false), //
                AttrConfigTestUtils.getAttr9(Category.FIRMOGRAPHICS, false));
        return renderedList;
    }

    private List<AttrConfig> generatePropertyListWithSomeUsedForSegment() {
        List<AttrConfig> renderedList = Arrays.asList(AttrConfigTestUtils.getAttr1(Category.INTENT, true, true),
                AttrConfigTestUtils.getAttr2(Category.INTENT, true, true), //
                AttrConfigTestUtils.getAttr3(Category.INTENT, true, true), //
                AttrConfigTestUtils.getAttr4(Category.INTENT, true, false), //
                AttrConfigTestUtils.getAttr5(Category.INTENT, false, true), //
                AttrConfigTestUtils.getAttr6(Category.INTENT, false, false), //
                AttrConfigTestUtils.getAttr7(Category.INTENT, false, false), //
                AttrConfigTestUtils.getAttr8(Category.INTENT, false, false), //
                AttrConfigTestUtils.getAttr9(Category.INTENT, false, false));
        return renderedList;
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
