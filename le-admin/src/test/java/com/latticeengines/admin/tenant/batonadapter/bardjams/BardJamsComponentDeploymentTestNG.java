package com.latticeengines.admin.tenant.batonadapter.bardjams;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;

public class BardJamsComponentDeploymentTestNG extends BatonAdapterBaseDeploymentTestNG<BardJamsComponent> {

    @Test(groups = "deployment")
    public void testInstallation() {
        bootstrap();

        // wait a while, then test your installation
    }

    @Override
    protected String getServiceName() { return BardJamsComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "bardjams_expected.json"; }
}
