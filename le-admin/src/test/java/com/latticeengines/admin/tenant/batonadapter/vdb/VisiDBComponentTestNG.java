package com.latticeengines.admin.tenant.batonadapter.vdb;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;

public class VisiDBComponentTestNG extends BatonAdapterBaseDeploymentTestNG {

    @Test(groups = "deployment")
    public void testInstallation() {
        bootstrap();

        // wait a while, then test your installation
    }

    @Override
    protected String getServiceName() { return VisiDBComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "vdb_expected.json"; }
}
