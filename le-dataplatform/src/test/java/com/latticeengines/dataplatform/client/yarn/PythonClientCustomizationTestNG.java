package com.latticeengines.dataplatform.client.yarn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.yarn.client.DefaultYarnClientCustomization;
import com.latticeengines.dataplatform.exposed.yarn.client.YarnClientCustomization;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class PythonClientCustomizationTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private PythonClientCustomization pythonClientCustomization;
    
    @Autowired
    private DefaultYarnClientCustomization defaultYarnClientCustomization;

    @Test(groups = "functional")
    public void PythonClientCustomization() {
        assertNotNull(pythonClientCustomization.getConfiguration());

        assertEquals(YarnClientCustomization.getCustomization(pythonClientCustomization.getClientId()),
                pythonClientCustomization);
    }

    @Test(groups = "functional")
    public void DefaultYarnClientCustomization() {
        assertNotNull(defaultYarnClientCustomization.getConfiguration());

        assertEquals(YarnClientCustomization.getCustomization(defaultYarnClientCustomization.getClientId()),
                defaultYarnClientCustomization);
    }
}
