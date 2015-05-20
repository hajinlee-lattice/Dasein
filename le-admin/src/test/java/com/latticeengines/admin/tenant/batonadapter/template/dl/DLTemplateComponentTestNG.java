package com.latticeengines.admin.tenant.batonadapter.template.dl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.service.impl.ComponentOrchestrator;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponentTestNG;
import com.latticeengines.domain.exposed.admin.DLRestResult;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class DLTemplateComponentTestNG extends BatonAdapterDeploymentTestNGBase{

    @Autowired
    private VisiDBDLComponentTestNG visiDBDLComponentTestNG;

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Autowired
    private TenantService tenantService;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    private String tenant;

    private String visiDBName;

    @BeforeClass(groups = { "deployment", "functional" })
    @Override
    public void setup() throws Exception {
        super.setup();
        visiDBName = "TestVisiDB";
        tenant = tenantId;
        SpaceConfiguration spaceConfig = tenantService.getTenant(contractId, tenantId).getSpaceConfig();
        spaceConfig.setDlAddress(dlUrl);
        tenantService.setupSpaceConfiguration(contractId, tenantId, spaceConfig);
    }

    public void installDLTemplate(){
        Map<String, Map<String, String>> properties = new HashMap<>();
        DocumentDirectory confDir = visiDBDLComponentTestNG.constructVisiDBDLInstaller(visiDBName);
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(confDir);
        properties.put(visiDBDLComponentTestNG.getServiceName(), sDir.flatten());

        sDir = new SerializableDocumentDirectory(
                batonService.getDefaultConfiguration(getServiceName()));
        properties.put(getServiceName(), sDir.flatten());
        orchestrator.orchestrate(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, properties);
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, ClientProtocolException, IOException {
        DLRestResult response = visiDBDLComponentTestNG.deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.getStatus(), 5);
        Assert.assertTrue(response.getErrorMessage().contains("does not exist"));

        installDLTemplate();
        BootstrapState state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK);
        response = visiDBDLComponentTestNG.deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.getStatus(), 3);
    }

    @Override
    protected String getServiceName() {
        return DLTemplateComponent.componentName;
    }

    @Override
    protected String getExpectedJsonFile() {
        return "dl_tpl_expected.json";
    }
}
