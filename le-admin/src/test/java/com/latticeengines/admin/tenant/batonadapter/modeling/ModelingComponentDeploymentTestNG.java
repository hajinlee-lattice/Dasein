package com.latticeengines.admin.tenant.batonadapter.modeling;

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

public class ModelingComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    private int userFeaturesThreshold = 10;
    private int defaultFeaturesThreshold = -1;

    private boolean enableEncryptData = true;
    private boolean defaultEnableEncryptData = false;

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, IOException {
        bootstrap(batonService.getDefaultConfiguration(ModelingComponent.componentName));

        BootstrapState state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        SerializableDocumentDirectory configured = tenantService.getTenantServiceConfig(contractId, tenantId,
                getServiceName());
        SerializableDocumentDirectory.Node node = configured.getNodeAtPath("/FeaturesThreshold");
        Assert.assertEquals(Integer.parseInt(node.getData()), defaultFeaturesThreshold);
        node = configured.getNodeAtPath("/EncryptData");
        Assert.assertEquals(Boolean.getBoolean(node.getData()), defaultEnableEncryptData);

        // idempotent test
        Path servicePath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), contractId,
                tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, getServiceName());
        try {
            CamilleEnvironment.getCamille().delete(servicePath);
        } catch (Exception e) {
            // ignore
        }

        bootstrap(getModelingDocumentDirectory());
        state = waitUntilStateIsNotInitial(contractId, tenantId, ModelingComponent.componentName);
        try {
            Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        } catch (AssertionError e) {
            Assert.fail("Idempotent test failed.", e);
        }
        configured = tenantService.getTenantServiceConfig(contractId, tenantId, getServiceName());
        node = configured.getNodeAtPath("/FeaturesThreshold");
        Assert.assertEquals(Integer.parseInt(node.getData()), userFeaturesThreshold);
        node = configured.getNodeAtPath("/EncryptData");

        String data = node.getData();
        System.out.println("After boostrapping, EncryptData is " + data);
        for (int i = 0; i < data.length(); i++) {
            System.out.println(data.charAt(i));
        }
        Assert.assertEquals(Boolean.parseBoolean(node.getData()), enableEncryptData);
    }

    @Override
    protected String getServiceName() {
        return ModelingComponent.componentName;
    }

    public DocumentDirectory getModelingDocumentDirectory() {
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/FeaturesThreshold"));
        node.getDocument().setData(String.valueOf(userFeaturesThreshold));
        node = confDir.get(new Path("/EncryptData"));
        node.getDocument().setData(String.valueOf(enableEncryptData));

        return confDir;
    }

}
