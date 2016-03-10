package com.latticeengines.admin.tenant.batonadapter.modeling;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
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

    private final static Log log = LogFactory.getLog(ModelingComponentDeploymentTestNG.class);

    private int userFeaturesThreshold = 10;
    private int defaultFeaturesThreshold = -1;

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        log.info("Start tearing down public class ModelingComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase");
        super.tearDown();
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, IOException {
        bootstrap(batonService.getDefaultConfiguration(getServiceName()));
        BootstrapState state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        SerializableDocumentDirectory configured = tenantService.getTenantServiceConfig(contractId, tenantId,
                getServiceName());
        SerializableDocumentDirectory.Node node = configured.getNodeAtPath("/FeaturesThreshold");
        Assert.assertEquals(node.getData(), defaultFeaturesThreshold);

        // send to bootstrapper message queue
        bootstrap(getModelingDocumentDirectory());
        // wait a while, then test your installation
        state = waitUntilStateIsNotInitial(contractId, tenantId, ModelingComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        // idempotent test
        Path servicePath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), contractId,
                tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, PLSComponent.componentName);
        try {
            CamilleEnvironment.getCamille().delete(servicePath);
        } catch (Exception e) {
            // ignore
        }

        bootstrap(getModelingDocumentDirectory());
        state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        try {
            Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        } catch (AssertionError e) {
            Assert.fail("Idempotent test failed.", e);
        }
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
        node.getDocument().setData("\"" + String.valueOf(userFeaturesThreshold) + "\"");

        return confDir;
    }

}
