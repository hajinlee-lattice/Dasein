package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class VisiDBDLComponentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    @Value("${admin.test.vbservername}")
    private String visiDBServerName;

    @Value("${admin.test.dl.user}")
    private String ownerEmail;

    private String tenant;

    private String createNewVisiDB;

    private String visiDBName;

    @BeforeClass(groups = { "deployment", "functional" })
    @Override
    public void setup() throws Exception {
        super.setup();
        createNewVisiDB = "true";
        visiDBName = "TestVisiDB";
        tenant = tenantService.getTenant(contractId, tenantId).getTenantInfo().properties.displayName;
        tenantService.getTenant(contractId, tenantId).getSpaceConfig().setDlAddress(dlUrl);
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, ClientProtocolException, IOException {
        JsonNode response = deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.get("Status").asInt(), 5);
        Assert.assertTrue(response.get("ErrorMessage").asText().contains("does not exist"));

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/VisiDB"));
        node.getChild("CreateNewVisiDB").getDocument().setData(createNewVisiDB);
        node = confDir.get(new Path("/VisiDB"));
        node.getChild("VisiDBName").getDocument().setData(visiDBName);
        node = confDir.get(new Path("/VisiDB"));
        node.getChild("ServerName").getDocument().setData(visiDBServerName);
        node = confDir.get(new Path("/DL"));
        node.getChild("OwnerEmail").getDocument().setData(ownerEmail);

        bootstrap(confDir);

        int numOfRetries = 10;
        BootstrapState state;
        do {
            state = batonService.getTenantServiceBootstrapState(contractId, tenantId, "VisiDBDL");
            numOfRetries--;
            Thread.sleep(1000L);
        } while (state.state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);

        if (!state.state.equals(BootstrapState.State.OK)) {
            System.out.println(state.errorMessage);
        }

        Assert.assertEquals(state.state, BootstrapState.State.OK);
        response = deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.get("Status").asInt(), 3);

    }

    @Test(groups = "functional")
    public void testInstallationFunctional() throws InterruptedException {

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/VisiDB"));
        node.getChild("CreateNewVisiDB").getDocument().setData(createNewVisiDB);
        node = confDir.get(new Path("/VisiDB"));
        node.getChild("VisiDBName").getDocument().setData(visiDBName);
        node = confDir.get(new Path("/VisiDB"));
        node.getChild("ServerName").getDocument().setData(visiDBServerName);
        node = confDir.get(new Path("/DL"));
        node.getChild("OwnerEmail").getDocument().setData(ownerEmail);

        // send to bootstrapper message queue
        bootstrap(confDir);

        // wait a while, then test your installation
        int numOfRetries = 10;
        BootstrapState.State state;
        do {
            state = batonService.getTenantServiceBootstrapState(contractId, tenantId, "VisiDBDL").state;
            numOfRetries--;
            Thread.sleep(1000L);
        } while (state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);

        Assert.assertEquals(state, BootstrapState.State.OK);

        SerializableDocumentDirectory sDir = tenantService.getTenantServiceConfig(contractId, tenantId, "VisiDBDL");

        for (SerializableDocumentDirectory.Node sNode : sDir.getNodes()) {
            if (sNode.getNode().equals("TenantAlias")) {
                Assert.assertEquals(sNode.getData(), "");
            }
        }

    }

    private JsonNode deleteVisiDBDLTenant(String tenant) throws ClientProtocolException, IOException {
        DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenant, "3");
        String jsonStr = JsonUtils.serialize(request);
        VisiDBDLInstaller installer = new VisiDBDLInstaller();
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/DeleteDLTenant",
                false, installer.getHeaders(), jsonStr);
        return installer.convertToJsonNode(response);
    }

    @Override
    protected String getServiceName() {
        return VisiDBDLComponent.componentName;
    }

    @Override
    public String getExpectedJsonFile() {
        return "vdbdl_expected.json";
    }
}
