package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class VisiDBDLComponentTestNG extends BatonAdapterBaseDeploymentTestNG {

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
    protected String getServiceName() { return VisiDBDLComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "vdbdl_expected.json"; }
}
