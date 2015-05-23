package com.latticeengines.admin.tenant.batonadapter.template.visidb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.impl.ComponentOrchestrator;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponentTestNG;
import com.latticeengines.domain.exposed.admin.DLRestResult;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class VisiDBTemplateComponentTestNG extends VisiDBDLComponentTestNG {

    @Autowired
    private VisiDBDLComponentTestNG visiDBDLComponentTestNG;

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    @Value("${admin.mount.dl.datastore}")
    private String dataStore;

    @Value("${admin.test.dl.datastore.server}")
    private String dataStoreServer;

    public void installVisiDBTemplate(){
        Map<String, Map<String, String>> properties = new HashMap<>();
        DocumentDirectory confDir = visiDBDLComponentTestNG.constructVisiDBDLInstaller();
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(confDir);
        properties.put(visiDBDLComponentTestNG.getServiceName(), sDir.flatten());

        sDir = new SerializableDocumentDirectory(
                batonService.getDefaultConfiguration(getServiceName()));
        properties.put(getServiceName(), sDir.flatten());
        orchestrator.orchestrate(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, properties);
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, IOException {
        DLRestResult response = visiDBDLComponentTestNG.deleteVisiDBDLTenantWithRetry(tenant);
        Assert.assertEquals(response.getStatus(), 5);
        Assert.assertTrue(response.getErrorMessage().contains("does not exist"));

        installVisiDBTemplate();
        // verify parent component, for debugging purpose
        BootstrapState state = waitForSuccess(VisiDBDLComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        state = waitForSuccess(getServiceName());
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        response = visiDBDLComponentTestNG.deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.getStatus(), 3);
    }

    @Override
    public String getServiceName() {
        return VisiDBTemplateComponent.componentName;
    }

    @Override
    public String getExpectedJsonFile() {
        return "vdb_tpl_expected.json";
    }
    
}
