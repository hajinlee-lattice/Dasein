package com.latticeengines.dataplatform.client.yarn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.yarn.exposed.client.DefaultYarnClientCustomization;
import com.latticeengines.yarn.exposed.client.YarnClientCustomization;
public class PythonClientCustomizationTestNG extends DataPlatformFunctionalTestNGBase {

    @Inject
    private PythonClientCustomization pythonClientCustomization;

    @Inject
    private DefaultYarnClientCustomization defaultYarnClientCustomization;

    @Test(groups = "functional.platform")
    public void PythonClientCustomization() {
        assertNotNull(pythonClientCustomization.getConfiguration());

        assertEquals(YarnClientCustomization.getCustomization(pythonClientCustomization.getClientId()),
                pythonClientCustomization);
    }

    @Test(groups = "functional.platform")
    public void DefaultYarnClientCustomization() {
        assertNotNull(defaultYarnClientCustomization.getConfiguration());

        assertEquals(YarnClientCustomization.getCustomization(defaultYarnClientCustomization.getClientId()),
                defaultYarnClientCustomization);
    }
}
