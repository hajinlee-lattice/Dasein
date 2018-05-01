package com.latticeengines.apps.core.service.impl;

import static org.mockito.Mockito.doReturn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
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
