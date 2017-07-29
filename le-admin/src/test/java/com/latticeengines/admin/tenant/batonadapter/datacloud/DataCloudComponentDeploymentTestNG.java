package com.latticeengines.admin.tenant.batonadapter.datacloud;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class DataCloudComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, IOException {
        bootstrap(batonService.getDefaultConfiguration(getServiceName()));

        BootstrapState state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        SerializableDocumentDirectory configured = tenantService.getTenantServiceConfig(contractId, tenantId,
                getServiceName());
        SerializableDocumentDirectory.Node node = configured.getNodeAtPath("/BypassDnBCache");
        Assert.assertFalse(Boolean.valueOf(node.getData()));

        // idempotent test
        Path servicePath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), contractId,
                tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, getServiceName());
        try {
            CamilleEnvironment.getCamille().delete(servicePath);
        } catch (Exception e) {
            // ignore
        }

        bootstrap(getDataCloudDocumentDirectory());
        state = waitUntilStateIsNotInitial(contractId, tenantId, getServiceName());
        try {
            Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        } catch (AssertionError e) {
            Assert.fail("Idempotent test failed.", e);
        }
        configured = tenantService.getTenantServiceConfig(contractId, tenantId, getServiceName());
        node = configured.getNodeAtPath("/BypassDnBCache");
        Assert.assertTrue(Boolean.valueOf(node.getData()));
    }

    @Override
    protected String getServiceName() {
        return DataCloudComponent.componentName;
    }

    private DocumentDirectory getDataCloudDocumentDirectory() {
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/BypassDnBCache"));
        node.getDocument().setData(String.valueOf(true));

        return confDir;
    }
}
