package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;

public class GenericAndUsageValidatorUnitTestNG {

    @Spy
    private GenericValidator genericValidator;
    @Spy
    private UsageValidator usageValidator;

    @BeforeClass(groups = "unit")
    private void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(groups = "unit")
    public void testGeneric() {
        AttrConfig lDCInternal = AttrConfigTestUtils.getLDCInternalAttr(Category.GROWTH_TRENDS, false, true, false,
                false, false);
        List<AttrConfig> attrList = Collections.singletonList(lDCInternal);
        genericValidator.validate(new ArrayList<>(), attrList, new AttrValidation());
        Assert.assertNull(lDCInternal.getValidationErrors());
        lDCInternal = AttrConfigTestUtils.getLDCInternalAttr(Category.GROWTH_TRENDS, false, false, false, false, true);
        genericValidator.validate(new ArrayList<>(), Collections.singletonList(lDCInternal), new AttrValidation());
        Assert.assertNotNull(lDCInternal.getValidationErrors());
        Assert.assertTrue(lDCInternal.getValidationErrors().getErrors().containsKey(ValidationErrors.Type.INVALID_PROP_CHANGE));

        attrList = AttrConfigTestUtils.generatePropertyList(Category.FIRMOGRAPHICS, true, true, true, true, true);
        genericValidator.validate(new ArrayList<>(), attrList, new AttrValidation());
        int num = AttrConfigTestUtils.getErrorNumber(attrList);
        Assert.assertEquals(attrList.size() - 4, num);

        attrList = AttrConfigTestUtils.generatePropertyList(Category.FIRMOGRAPHICS, false, false, false, false, false);
        genericValidator.validate(new ArrayList<>(), attrList, new AttrValidation());
        num = AttrConfigTestUtils.getErrorNumber(attrList);
        Assert.assertEquals(0, num);
    }

    @Test(groups = "unit")
    public void testUsage() {
        List<AttrConfig> attrList = AttrConfigTestUtils.generatePropertyList(Category.FIRMOGRAPHICS, true, true, true,
                true, true);
        usageValidator.validate(new ArrayList<>(), attrList, new AttrValidation());
        int num = AttrConfigTestUtils.getErrorNumber(attrList);
        Assert.assertEquals(attrList.size() - 8, num);
        attrList = AttrConfigTestUtils.generatePropertyList(Category.FIRMOGRAPHICS, false, false, false, false, false);
        genericValidator.validate(new ArrayList<>(), attrList, new AttrValidation());
        num = AttrConfigTestUtils.getErrorNumber(attrList);
        Assert.assertEquals(0, num);
    }

}
