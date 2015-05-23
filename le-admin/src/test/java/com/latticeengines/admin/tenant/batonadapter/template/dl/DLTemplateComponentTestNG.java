package com.latticeengines.admin.tenant.batonadapter.template.dl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.service.impl.ComponentOrchestrator;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponentTestNG;
import com.latticeengines.domain.exposed.admin.DLRestResult;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class DLTemplateComponentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private VisiDBDLComponentTestNG visiDBDLComponentTestNG;

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Autowired
    private TenantService tenantService;

    @Value("${admin.test.vdb.servername}")
    private String visiDBServerName;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    @Value("${admin.mount.dl.datastore}")
    private String dataStore;

    @Value("${admin.test.dl.datastore.server}")
    private String dataStoreServer;

    @Value("${admin.mount.vdb.permstore}")
    private String permStore;

    @Value("${admin.test.vdb.permstore.server}")
    private String permStoreServer;

    private String tenant;

    @BeforeClass(groups = { "deployment", "functional" })
    @Override
    public void setup() throws Exception {
        super.setup();
        tenant = tenantId;
        SpaceConfiguration spaceConfig = tenantService.getTenant(contractId, tenantId).getSpaceConfig();
        spaceConfig.setDlAddress(dlUrl);
        tenantService.setupSpaceConfiguration(contractId, tenantId, spaceConfig);

        visiDBDLComponentTestNG.deleteVisiDBDLTenant(tenant);
        visiDBDLComponentTestNG.clearDatastore(dataStoreServer, permStoreServer, visiDBServerName, tenant);
    }

    @AfterClass(groups = {"deployment", "functional"})
    @Override
    public void tearDown() throws Exception {
        visiDBDLComponentTestNG.deleteVisiDBDLTenant(tenant);
        visiDBDLComponentTestNG.clearDatastore(dataStoreServer, permStoreServer, visiDBServerName, tenant);
        super.tearDown();
    }

    public void installDLTemplate(){
        Map<String, Map<String, String>> properties = new HashMap<>();
        DocumentDirectory confDir = visiDBDLComponentTestNG.constructVisiDBDLInstaller();
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(confDir);
        properties.put(VisiDBDLComponent.componentName, sDir.flatten());

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

        installDLTemplate();
        // verify parent component, for debugging purpose
        BootstrapState state = waitForSuccess(VisiDBDLComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK);
        response = visiDBDLComponentTestNG.deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.getStatus(), 3);
    }

    @Override
    public String getServiceName() {
        return DLTemplateComponent.componentName;
    }

    @Override
    public String getExpectedJsonFile() {
        return "dl_tpl_expected.json";
    }
}
