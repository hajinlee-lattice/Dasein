package com.latticeengines.admin.tenant.batonadapter.template;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.template.dl.DLTemplateComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TemplateComponentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Test(groups = "functional")
    public void testDLInstallation() {
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
                tenantService.getTenantServiceConfig(contractId, tenantId, DLTemplateComponent.componentName);
        
        Assert.assertNotNull(resultDir);

    }

    @Override
    protected String getServiceName() { return DLTemplateComponent.componentName; }

    @Override
    public String getExpectedJsonFile() { return "dl_tpl_expected.json"; }

}
