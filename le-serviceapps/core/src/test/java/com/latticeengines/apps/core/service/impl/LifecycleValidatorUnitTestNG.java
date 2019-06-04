package com.latticeengines.apps.core.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ImpactWarnings;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;

public class LifecycleValidatorUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(LifecycleValidatorUnitTestNG.class);
    @Spy
    private LifecycleValidator lifecycleValidator;

    @BeforeClass(groups = "unit")
    private void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = "unit")
    private void testLifeCycle() {
        AttrConfig lDCPremium = AttrConfigTestUtils.getLDCPremiumAttr(Category.INTENT, true);
        lDCPremium.setShouldDeprecate(true);
        ((AttrConfigProp<AttrState>) lDCPremium.getProperty(ColumnMetadataKey.State))
                .setSystemValue(AttrState.Deprecated);
        List<AttrConfig> attrList = Arrays.asList(lDCPremium);

        lifecycleValidator.validate(new ArrayList<>(), attrList, new AttrValidation());
        ValidationErrors error = lDCPremium.getValidationErrors();
        Assert.assertNotNull(lDCPremium.getValidationErrors());
        Assert.assertEquals(error.getErrors().keySet().contains(ValidationErrors.Type.INVALID_ACTIVATION), true);

        lDCPremium = AttrConfigTestUtils.getLDCPremiumAttr(Category.INTENT, false);
        AttrConfig lDCNonPremium = AttrConfigTestUtils.getLDCNonPremiumAttr(Category.INTENT, false);
        AttrConfig cDLAccount = AttrConfigTestUtils.getCDLAccountExtensionAttr(Category.INTENT, false);
        AttrConfig cDLContact = AttrConfigTestUtils.getCDLContactExtensionAttr(Category.INTENT, false);
        attrList = Arrays.asList(lDCPremium, lDCNonPremium, cDLAccount, cDLContact);
        lifecycleValidator.validate(new ArrayList<>(), attrList, new AttrValidation());
        Assert.assertNotNull(lDCPremium.getImpactWarnings());
        Assert.assertEquals(lDCPremium.getImpactWarnings().getWarnings().containsKey(ImpactWarnings.Type.USAGE_ENABLED),
                true);
        Assert.assertNotNull(lDCNonPremium.getImpactWarnings());
        Assert.assertEquals(
                lDCNonPremium.getImpactWarnings().getWarnings().containsKey(ImpactWarnings.Type.USAGE_ENABLED), true);
        Assert.assertNotNull(cDLAccount.getImpactWarnings());
        Assert.assertEquals(cDLAccount.getImpactWarnings().getWarnings().containsKey(ImpactWarnings.Type.USAGE_ENABLED),
                true);
        Assert.assertNotNull(cDLContact.getImpactWarnings());
        Assert.assertEquals(cDLContact.getImpactWarnings().getWarnings().containsKey(ImpactWarnings.Type.USAGE_ENABLED),
                true);

        lDCPremium = AttrConfigTestUtils.getLDCPremiumAttr(Category.INTENT, true);
        lDCNonPremium = AttrConfigTestUtils.getLDCNonPremiumAttr(Category.INTENT, true);
        cDLAccount = AttrConfigTestUtils.getCDLAccountExtensionAttr(Category.INTENT, true);
        cDLContact = AttrConfigTestUtils.getCDLContactExtensionAttr(Category.INTENT, true);
        attrList = Arrays.asList(lDCPremium, lDCNonPremium, cDLAccount, cDLContact);
        lifecycleValidator.validate(new ArrayList<>(), attrList, new AttrValidation());
        Assert.assertNull(lDCPremium.getImpactWarnings());
        Assert.assertNull(lDCNonPremium.getImpactWarnings());
        Assert.assertNull(cDLAccount.getImpactWarnings());
        Assert.assertNull(cDLContact.getImpactWarnings());
        Assert.assertNull(lDCPremium.getValidationErrors());
        Assert.assertNull(lDCNonPremium.getValidationErrors());
        Assert.assertNull(cDLAccount.getValidationErrors());
        Assert.assertNull(cDLContact.getValidationErrors());
    }
}
