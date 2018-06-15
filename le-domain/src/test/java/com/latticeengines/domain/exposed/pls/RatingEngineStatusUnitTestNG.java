package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RatingEngineStatusUnitTestNG {

    @Test(groups = "unit")
    public void testTransition() {
        Assert.assertTrue(RatingEngineStatus.INACTIVE.canTransition(RatingEngineStatus.ACTIVE));
        Assert.assertTrue(RatingEngineStatus.ACTIVE.canTransition(RatingEngineStatus.INACTIVE));
    }
}
