package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

public class AttrConfigServiceImplUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImplUnitTestNG.class);

    @Mock
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @InjectMocks
    private AttrConfigServiceImpl attrConfigService;

    private static final Long intentLimit = 500L;
    private static final Long activeForIntent = 5000L;
    private static final Long inactiveForIntent = 4000L;
    private static final Long totalIntentAttrs = 90000L;
    private static Tenant tenant;

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

    @Test(groups = "unit", dependsOnMethods = { "testGetAttrConfigActivationOverview" })
    public void testGetAttrConfigUsageOverview() {
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                ColumnSelection.Predefined.Segment.getName()))
                        .thenReturn(generatePropertyAttrConfigOverview(ColumnSelection.Predefined.Segment.getName()));
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                ColumnSelection.Predefined.Enrichment.getName())).thenReturn(
                        generatePropertyAttrConfigOverview(ColumnSelection.Predefined.Enrichment.getName()));
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                ColumnSelection.Predefined.CompanyProfile.getName()))
                        .thenReturn(generatePropertyAttrConfigOverview(
                                ColumnSelection.Predefined.Segment.CompanyProfile.getName()));
        when(cdlAttrConfigProxy.getAttrConfigOverview(tenant.getId(), null,
                ColumnSelection.Predefined.TalkingPoint.getName())).thenReturn(
                        generatePropertyAttrConfigOverview(ColumnSelection.Predefined.Segment.TalkingPoint.getName()));
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
