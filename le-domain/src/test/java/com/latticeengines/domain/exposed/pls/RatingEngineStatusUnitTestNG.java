package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RatingEngineStatusUnitTestNG {

    @Test(groups = "unit")
    public void testTransition() {
        Assert.assertTrue(RatingEngineStatus.canTransit(RatingEngineStatus.INACTIVE, RatingEngineStatus.ACTIVE));
        Assert.assertTrue(RatingEngineStatus.canTransit(RatingEngineStatus.INACTIVE, RatingEngineStatus.DELETED));
        Assert.assertTrue(RatingEngineStatus.canTransit(RatingEngineStatus.ACTIVE, RatingEngineStatus.INACTIVE));
        Assert.assertTrue(RatingEngineStatus.canTransit(RatingEngineStatus.DELETED, RatingEngineStatus.INACTIVE));
        Assert.assertFalse(RatingEngineStatus.canTransit(RatingEngineStatus.DELETED, RatingEngineStatus.ACTIVE));
        Assert.assertFalse(RatingEngineStatus.canTransit(RatingEngineStatus.ACTIVE, RatingEngineStatus.DELETED));
    }
}
