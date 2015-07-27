package com.latticeengines.admin.tenant.batonadapter.dante;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class DanteComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Value("${admin.dante.dryrun}")
    private boolean danteSkipped;

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException {
        if (danteSkipped) { return; }

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // send to bootstrapper message queue
        bootstrap(confDir);
        // wait a while, then test your installation
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, getServiceName());
        Assert.assertTrue(BootstrapState.State.OK.equals(state.state)
                || BootstrapState.State.INITIAL.equals(state.state), state.errorMessage);

        // idempotent test
        deleteDanteTenantFromZK();
        bootstrap(confDir);
        state = waitUntilStateIsNotInitial(contractId, tenantId, getServiceName());
        try {
            Assert.assertTrue(BootstrapState.State.OK.equals(state.state)
                    || BootstrapState.State.INITIAL.equals(state.state), state.errorMessage);
        } catch (AssertionError e) {
            Assert.fail("Idempotent test failed.", e);
        }
    }

    @Override
    protected String getServiceName() { return DanteComponent.componentName; }

    private void deleteDanteTenantFromZK() {
        Path servicePath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, getServiceName());
        try {
            CamilleEnvironment.getCamille().delete(servicePath);
        } catch (Exception e) {
            // ignore
        }
    }
}
