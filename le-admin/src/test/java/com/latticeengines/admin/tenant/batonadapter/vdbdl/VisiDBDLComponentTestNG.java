package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.DLRestResult;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

@Component
public class VisiDBDLComponentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    @Value("${admin.test.vdbservername}")
    private String visiDBServerName;

    @Value("${admin.test.dl.user}")
    private String ownerEmail;

    private String tenant;

    private String visiDBName;

    @BeforeClass(groups = { "deployment", "functional" })
    @Override
    public void setup() throws Exception {
        super.setup();
        visiDBName = "TestVisiDB";
        tenant = String.format("%s.%s.%s", contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        SpaceConfiguration spaceConfig = tenantService.getTenant(contractId, tenantId).getSpaceConfig();
        spaceConfig.setDlAddress(dlUrl);
        tenantService.setupSpaceConfiguration(contractId, tenantId, spaceConfig);
    }

    public DocumentDirectory constructVisiDBDLInstaller(String visiDBName){
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();
        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/VisiDB"));
        node = confDir.get(new Path("/VisiDB"));
        node.getChild("VisiDBName").getDocument().setData(visiDBName);
        node = confDir.get(new Path("/VisiDB"));
        node.getChild("ServerName").getDocument().setData(visiDBServerName);
        node = confDir.get(new Path("/DL"));
        node.getChild("OwnerEmail").getDocument().setData(ownerEmail);
        return confDir;
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, ClientProtocolException, IOException {
        DLRestResult response = deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.getStatus(), 5);
        Assert.assertTrue(response.getErrorMessage().contains("does not exist"));

        bootstrap(constructVisiDBDLInstaller(visiDBName));
        BootstrapState state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK);
        response = deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.getStatus(), 3);
    }

    @Test(groups = "functional")
    public void testInstallationFunctional() throws InterruptedException, ClientProtocolException, IOException {
        bootstrap(constructVisiDBDLInstaller(visiDBName));

        // wait a while, then test your installation
        BootstrapState state = waitForSuccess(getServiceName());
        Assert.assertEquals(state.state, BootstrapState.State.OK);

        SerializableDocumentDirectory sDir = tenantService.getTenantServiceConfig(contractId, tenantId, getServiceName());

        for (SerializableDocumentDirectory.Node sNode : sDir.getNodes()) {
            if (sNode.getNode().equals("TenantAlias")) {
                Assert.assertEquals(sNode.getData(), "");
            }
        }
    }

    public DLRestResult deleteVisiDBDLTenant(String tenant) throws ClientProtocolException, IOException {
        DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenant, "3");
        String jsonStr = JsonUtils.serialize(request);
        VisiDBDLInstaller installer = new VisiDBDLInstaller();
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/DeleteDLTenant",
                false, installer.getHeaders(), jsonStr);
        return JsonUtils.deserialize(response, DLRestResult.class);
    }

    @Override
    public String getServiceName() {
        return VisiDBDLComponent.componentName;
    }

    @Override
    public String getExpectedJsonFile() {
        return "vdbdl_expected.json";
    }
}
