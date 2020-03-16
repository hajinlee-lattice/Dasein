package com.latticeengines.admin.tenant.batonadapter.dcp;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.cdl.CDLComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponentDeploymentTestNG;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

@Component
public class DCPComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Value("${common.test.microservice.url}")
    private String microserviceUrl;

    @Inject
    private PLSComponentDeploymentTestNG plsComponentDeploymentTestNG;

    @Override
    protected String getServiceName() {
        return DCPComponent.componentName;
    }

    @Test(groups = "deployment")
    public void testInstallation() {
        loginAD();
        // pls
        bootstrap(contractId, tenantId, PLSComponent.componentName, plsComponentDeploymentTestNG.getPLSDocumentDirectory());
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        // cdl
        bootstrap(contractId, tenantId, CDLComponent.componentName,
                batonService.getDefaultConfiguration(CDLComponent.componentName));
        state = waitUntilStateIsNotInitial(contractId, tenantId, CDLComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        // dcp
        bootstrap(contractId, tenantId, DCPComponent.componentName,
                batonService.getDefaultConfiguration(getServiceName()));
        state = waitUntilStateIsNotInitial(contractId, tenantId, DCPComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        // idempotent test for dcp
        bootstrap(contractId, tenantId, DCPComponent.componentName,
                batonService.getDefaultConfiguration(getServiceName()));
        state = waitUntilStateIsNotInitial(contractId, tenantId, DCPComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

    }

}
