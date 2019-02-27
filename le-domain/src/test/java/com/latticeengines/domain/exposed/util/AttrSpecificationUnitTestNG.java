package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceapps.core.AttrSpecification;

public class AttrSpecificationUnitTestNG {

    @Test(groups = "unit")
    public void test(){
        AttrSpecification specification = AttrSpecification.LDC_NON_PREMIUM();
        Assert.assertTrue(specification.segmentationChange());
        specification.setSegmentationChange(false);
        Assert.assertFalse(specification.segmentationChange());
        AttrSpecification specification2 = AttrSpecification.LDC_NON_PREMIUM();
        Assert.assertTrue(specification2.segmentationChange());
    }

}
