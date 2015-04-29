package com.latticeengines.admin.tenant.batonadapter.template;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TemplateComponentTestNG extends BatonAdapterBaseDeploymentTestNG<TemplateComponent> {
    
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
    protected String getServiceName() { return TemplateComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "tpl_expected.json"; }

}
