package com.latticeengines.admin.tenant.batonadapter.vdb;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class VisiDBComponentTestNG extends BatonAdapterBaseDeploymentTestNG {

    @Test(groups = "deployment")
    public void testInstallation() {
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());

        // modify the default config
        // ...

        // send to bootstrapper message queue
        bootstrap(confDir);

        // wait a while, then test your installation
        // ...

    }

    @Override
    protected String getServiceName() { return VisiDBComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "vdb_expected.json"; }
}
