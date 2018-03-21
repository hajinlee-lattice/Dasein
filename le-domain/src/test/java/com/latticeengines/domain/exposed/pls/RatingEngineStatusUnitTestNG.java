package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RatingEngineStatusUnitTestNG {

    @Test(groups = "unit")
    public void testTransition() {
        Assert.assertTrue(RatingEngineStatus.canTransit(RatingEngineStatus.INACTIVE, RatingEngineStatus.ACTIVE));
        Assert.assertTrue(RatingEngineStatus.canTransit(RatingEngineStatus.ACTIVE, RatingEngineStatus.INACTIVE));
    }
}
