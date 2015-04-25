package com.latticeengines.admin.tenant.batonadapter.template;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;

public class TemplateComponentTestNG extends BatonAdapterBaseDeploymentTestNG {
    
    @Test(groups = "deployment")
    public void testInstallation() {
        bootstrap();

        // wait a while, then test your installation
    }

    @Override
    protected String getServiceName() { return TemplateComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "tpl_expected.json"; }

}
