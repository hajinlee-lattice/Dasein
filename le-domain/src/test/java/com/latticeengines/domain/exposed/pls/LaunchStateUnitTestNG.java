package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LaunchStateUnitTestNG {

    @Test(groups = "unit")
    public void testCanTransit() {
        Assert.assertTrue(LaunchState.canTransit(LaunchState.Launching, LaunchState.Launched));
        Assert.assertTrue(LaunchState.canTransit(LaunchState.Launching, LaunchState.Failed));
        Assert.assertTrue(LaunchState.canTransit(LaunchState.Launching, LaunchState.Deleted));
        Assert.assertTrue(LaunchState.canTransit(LaunchState.Launching, LaunchState.Canceled));
        Assert.assertFalse(LaunchState.canTransit(LaunchState.Canceled, LaunchState.Launching));
        Assert.assertFalse(LaunchState.canTransit(null, LaunchState.Canceled));
    }
}
