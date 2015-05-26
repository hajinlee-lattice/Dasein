package com.latticeengines.camille.featureflags;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
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
        final String id = "TestFlag";
        Assert.assertFalse(FeatureFlagClient.isEnabled(CamilleTestEnvironment.getCustomerSpace(), id));
        FeatureFlagClient.setDefinition(id, new FeatureFlagDefinition());
        FeatureFlagClient.setEnabled(CamilleTestEnvironment.getCustomerSpace(), id, true);
        Assert.assertTrue(FeatureFlagClient.isEnabled(CamilleTestEnvironment.getCustomerSpace(), id));
    }
}
