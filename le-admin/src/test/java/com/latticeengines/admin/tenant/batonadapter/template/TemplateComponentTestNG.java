package com.latticeengines.admin.tenant.batonadapter.template;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TemplateComponentTestNG extends BatonAdapterBaseDeploymentTestNG {

    @Autowired
    private TenantService tenantService;

    @Test(groups = "functional")
    public void testInstallation() {
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());

        // modify the default config
        // ...

        // send to bootstrapper message queue
        bootstrap(confDir);

        // wait a while, then test your installation
        // ...
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            //ignore
        }

        SerializableDocumentDirectory resultDir =
                tenantService.getTenantServiceConfig(contractId, tenantId, TemplateComponent.componentName);

    }

    @Override
    protected String getServiceName() { return TemplateComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "tpl_expected.json"; }

}
