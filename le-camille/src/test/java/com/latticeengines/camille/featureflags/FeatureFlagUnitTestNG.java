package com.latticeengines.camille.featureflags;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;

public class FeatureFlagUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testEnableFlag() throws Exception {
        final String id1 = "TestFlag1";
        Assert.assertFalse(FeatureFlagClient.isEnabled(CamilleTestEnvironment.getCustomerSpace(), id1));
        FeatureFlagDefinition featureFlag1 = new FeatureFlagDefinition();
        Assert.assertFalse(featureFlag1.getConfigurable());
        FeatureFlagClient.setDefinition(id1, featureFlag1);
        try {
            FeatureFlagClient.setEnabled(CamilleTestEnvironment.getCustomerSpace(), id1, true);
        } catch (Exception e) {
            Assert.fail("Feature flag TestFlag can be toggled because configurable has only the right to control UI in tennant console.");
        }

        final String id2 = "TestFlag2";
        Assert.assertFalse(FeatureFlagClient.isEnabled(CamilleTestEnvironment.getCustomerSpace(), id2));
        FeatureFlagDefinition featureFlag2 = new FeatureFlagDefinition();
        Assert.assertFalse(featureFlag2.getConfigurable());
        featureFlag2.setConfigurable(true);
        Assert.assertTrue(featureFlag2.getConfigurable());
        FeatureFlagClient.setDefinition(id2, featureFlag2);
        try {
            FeatureFlagClient.setEnabled(CamilleTestEnvironment.getCustomerSpace(), id2, true);
            Assert.assertTrue(FeatureFlagClient.isEnabled(CamilleTestEnvironment.getCustomerSpace(), id2));
        } catch (Exception e) {
            Assert.fail("Should NOT have thrown RuntimeException.");
        }
    }

    @Test(groups = "unit")
    public void testFeatureFlagClientHandlesInvalidPath() throws Exception {
        final String id = "TestFlag";
        Assert.assertFalse(FeatureFlagClient.isEnabled(CustomerSpace.parse("foo"), id));
    }
}
