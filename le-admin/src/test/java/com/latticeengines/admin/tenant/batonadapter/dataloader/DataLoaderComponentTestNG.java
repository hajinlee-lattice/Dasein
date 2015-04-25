package com.latticeengines.admin.tenant.batonadapter.dataloader;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;

public class DataLoaderComponentTestNG extends BatonAdapterBaseDeploymentTestNG<DataLoaderComponent> {

    @Test(groups = "deployment")
    public void testInstallation() {
        bootstrap();

        // wait a while, then test your installation
    }

    @Override
    protected String getServiceName() { return DataLoaderComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "dl_expected.json"; }
}
