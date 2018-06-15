package com.latticeengines.domain.exposed.util;

import com.latticeengines.domain.exposed.serviceapps.core.AttrSpecification;
import org.testng.Assert;
import org.testng.annotations.Test;

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
