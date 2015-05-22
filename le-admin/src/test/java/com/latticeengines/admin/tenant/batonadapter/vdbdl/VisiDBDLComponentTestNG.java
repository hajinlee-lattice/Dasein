package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
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
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

@Component
public class VisiDBDLComponentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    @Value("${admin.test.vdb.servername}")
    private String visiDBServerName;

    @Value("${admin.test.dl.user}")
    private String ownerEmail;

    @Value("${admin.mount.dl.datastore}")
    private String dataStore;

    @Value("${admin.test.dl.datastore.server}")
    private String dataStoreServer;

    private String tenant;

    @BeforeClass(groups = { "deployment", "functional" })
    @Override
    public void setup() throws Exception {
        super.setup();
        tenant = tenantId;
        SpaceConfiguration spaceConfig = tenantService.getTenant(contractId, tenantId).getSpaceConfig();
        spaceConfig.setDlAddress(dlUrl);
        tenantService.setupSpaceConfiguration(contractId, tenantId, spaceConfig);

        String url = String.format("%s/admin/internal/", getRestHostPort());
        magicRestTemplate.delete(url + "datastore/" + dataStoreServer + "/" + tenant);

    }

    @AfterClass(groups = {"deployment", "functional"})
    @Override
    public void tearDown() throws Exception {
        String url = String.format("%s/admin/internal/", getRestHostPort());
        magicRestTemplate.delete(url + "datastore/" + dataStoreServer + "/" + tenant);
        super.tearDown();
    }

    public DocumentDirectory constructVisiDBDLInstaller() {
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();
        // modify the default config
        DocumentDirectory.Node node;
        node = confDir.get(new Path("/VisiDB"));
        node.getChild("ServerName").getDocument().setData(visiDBServerName);
        node.getChild("CreateNewVisiDB").getDocument().setData("false");
        node = confDir.get(new Path("/DL"));
        node.getChild("OwnerEmail").getDocument().setData(ownerEmail);
        node.getChild("DataStore").getDocument().setData(dataStoreServer);
        return confDir;
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, IOException {
        DLRestResult response = deleteVisiDBDLTenantWithRetry(tenant);
        Assert.assertEquals(response.getStatus(), 5);
        Assert.assertTrue(response.getErrorMessage().contains("does not exist"));

        // record original number of files in permStore
        String url = String.format("%s/admin/internal/", getRestHostPort());

        bootstrap(constructVisiDBDLInstaller());
        BootstrapState state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK);

        // verify permstore and datastore
        Assert.assertEquals(magicRestTemplate.getForObject(
                url + "datastore/" + dataStoreServer + "/" + tenantId, List.class).size(), 3);

        response = deleteVisiDBDLTenant(tenant);
        Assert.assertEquals(response.getStatus(), 3);
    }

    @Test(groups = "functional")
    public void testInstallationFunctional() throws InterruptedException, IOException {
        bootstrap(constructVisiDBDLInstaller());

        // wait a while, then test your installation
        BootstrapState state = waitForSuccess(getServiceName());
        Assert.assertEquals(state.state, BootstrapState.State.OK);

        SerializableDocumentDirectory sDir = tenantService.getTenantServiceConfig(contractId, tenantId,
                getServiceName());

        for (SerializableDocumentDirectory.Node sNode : sDir.getNodes()) {
            if (sNode.getNode().equals("TenantAlias")) {
                Assert.assertEquals(sNode.getData(), "");
            }
        }
    }

    public DLRestResult deleteVisiDBDLTenant(String tenant) throws IOException {
        DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenant, "3");
        String jsonStr = JsonUtils.serialize(request);
        VisiDBDLInstaller installer = new VisiDBDLInstaller();
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/DeleteDLTenant",
                false, installer.getHeaders(), jsonStr);
        return JsonUtils.deserialize(response, DLRestResult.class);
    }

    public DLRestResult deleteVisiDBDLTenantWithRetry(String tenant) throws IOException, InterruptedException {
        int numOfRetry = 5;
        DLRestResult response;
        do {
            response = deleteVisiDBDLTenant(tenant);
            numOfRetry--;
            Thread.sleep(2000L);
        } while(numOfRetry >0 &&
                (response.getStatus() != 5 || !response.getErrorMessage().contains("does not exist")));
        return response;
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
