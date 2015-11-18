package com.latticeengines.admin.tenant.batonadapter.eai;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class EaiComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase{

    @Autowired
    private TenantService tenantService;

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, IOException {
        bootstrap(batonService.getDefaultConfiguration(getServiceName()));
        BootstrapState state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        SerializableDocumentDirectory configured = tenantService.getTenantServiceConfig(contractId, tenantId,
                getServiceName());
        SerializableDocumentDirectory.Node node = configured.getNodeAtPath("/SalesforceEndpointConfig/HttpClient/ConnectTimeout");
        Assert.assertEquals(node.getData(), "60000");
        node = configured.getNodeAtPath("/SalesforceEndpointConfig/HttpClient/ImportTimeout");
        Assert.assertEquals(node.getData(), "3600000");
    }

    @Override
    protected String getServiceName() {
        return EaiComponent.componentName;
    }

}
