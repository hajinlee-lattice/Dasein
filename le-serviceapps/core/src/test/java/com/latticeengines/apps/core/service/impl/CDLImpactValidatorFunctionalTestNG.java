package com.latticeengines.apps.core.service.impl;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.junit.Assert;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.proxy.exposed.cdl.CDLDependenciesProxy;

public class CDLImpactValidatorFunctionalTestNG extends ServiceAppsFunctionalTestNGBase {
    @Inject
    private CDLImpactValidator cdlImpactValidator;

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void testCDLImpact() throws Exception {
        Tenant tenant = new Tenant();
        tenant.setPid(1L);
        tenant.setId(this.getClass().getSimpleName());
        tenant.setName(this.getClass().getSimpleName());
        MultiTenantContext.setTenant(tenant);
        CDLDependenciesProxy cdlDependenciesProxy = Mockito.mock(CDLDependenciesProxy.class);
        Play play = new Play();
        play.setName("play1");
        List<Play> playList = Arrays.asList(play);
        when(cdlDependenciesProxy.getDependingPlays(anyString(), anyList())).thenReturn(playList);
        MetadataSegment seg = new MetadataSegment();
        List<MetadataSegment> segList = Arrays.asList(seg);
        when(cdlDependenciesProxy.getDependingSegments(anyString(), anyList())).thenReturn(segList);
        ReflectionTestUtils.setField(cdlImpactValidator, "cdlDependenciesProxy", cdlDependenciesProxy);
        AttrConfig lDCNonPremium = AttrConfigTestUtils.getLDCNonPremiumAttr(Category.INTENT, true);
        lDCNonPremium.getStrongTypedProperty(ColumnSelection.Predefined.TalkingPoint.name(), Boolean.class).setCustomValue(false);
        cdlImpactValidator.validate(new ArrayList<>(), Arrays.asList(lDCNonPremium),
                new AttrValidation());

        Assert.assertNotNull(lDCNonPremium.getImpactWarnings());
        Assert.assertEquals(
                lDCNonPremium.getImpactWarnings().getWarnings().containsKey(ImpactWarnings.Type.IMPACTED_PLAYS), true);

        AttrConfig lDCPremium = AttrConfigTestUtils.getLDCPremiumAttr(Category.INTENT, true);
        lDCNonPremium.getStrongTypedProperty(ColumnSelection.Predefined.Segment.name(), Boolean.class).setCustomValue(false);
        cdlImpactValidator.validate(new ArrayList<>(), Arrays.asList(lDCPremium), new AttrValidation());

        Assert.assertNotNull(lDCPremium.getImpactWarnings());
        Assert.assertEquals(
                lDCPremium.getImpactWarnings().getWarnings().containsKey(ImpactWarnings.Type.IMPACTED_SEGMENTS),
                true);
    }
}
