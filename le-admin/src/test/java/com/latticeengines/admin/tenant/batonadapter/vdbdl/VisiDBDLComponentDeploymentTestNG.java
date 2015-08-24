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
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.GetVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component
public class VisiDBDLComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private DataLoaderService dataLoaderService;

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

    @Value("${admin.test.dl.datastore.path}")
    private String dataStorePath;

    @Value("${admin.mount.vdb.permstore}")
    private String permStore;

    @Value("${admin.test.vdb.permstore.server}")
    private String permStoreServer;

    protected String tenant;

    @BeforeClass(groups = "deployment")
    @Override
    public void setup() throws Exception {
        super.setup();
        tenant = tenantId;
        SpaceConfiguration spaceConfig = tenantService.getTenant(contractId, tenantId).getSpaceConfig();
        spaceConfig.setDlAddress(dlUrl);
        tenantService.setupSpaceConfiguration(contractId, tenantId, spaceConfig);

        deleteVisiDBDLTenantWithRetry(tenant);
        clearDatastore(dataStoreServer, permStoreServer, visiDBServerName, tenant);
    }

    @AfterClass(groups = "deployment")
    @Override
    public void tearDown() throws Exception {
        deleteVisiDBDLTenantWithRetry(tenant);
        clearDatastore(dataStoreServer, permStoreServer, visiDBServerName, tenant);
        super.tearDown();
    }

    public DocumentDirectory constructVisiDBDLInstaller() {
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();
        // modify the default config
        DocumentDirectory.Node node;
        node = confDir.get(new Path("/VisiDB"));
        node.getChild("ServerName").getDocument().setData(visiDBServerName);
        node.getChild("PermanentStore").getDocument().setData("\\\\" + permStoreServer + "\\VisiDB\\PermanentStore");
        node = confDir.get(new Path("/DL"));
        node.getChild("OwnerEmail").getDocument().setData(ownerEmail);
        node.getChild("DataStore").getDocument().setData(dataStorePath);
        return confDir;
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException, IOException {
        InstallResult response = deleteVisiDBDLTenantWithRetry(tenant);
        Assert.assertEquals(response.getStatus(), 5);
        Assert.assertTrue(response.getErrorMessage().contains("does not exist"));
        verifyTenant(tenant, dlUrl, false);

        // record original number of files in permStore
        String url = String.format("%s/admin/internal/", getRestHostPort());

        bootstrap(constructVisiDBDLInstaller());
        BootstrapState state = waitForSuccess(getServiceName());

        Assert.assertEquals(state.state, BootstrapState.State.OK);

        verifyTenant(tenant, dlUrl);
        // verify permstore and datastore
        Assert.assertEquals(
                magicRestTemplate.getForObject(url + "datastore/" + dataStoreServer + "/" + tenantId, List.class)
                        .size(), 3);
        // verify auto filled visidbname and tenantalias
        SerializableDocumentDirectory configured = tenantService.getTenantServiceConfig(contractId, tenantId,
                getServiceName());
        SerializableDocumentDirectory.Node node = configured.getNodeAtPath("/TenantAlias");
        Assert.assertEquals(node.getData(), tenantId);
        node = configured.getNodeAtPath("/VisiDB/VisiDBName");
        Assert.assertEquals(node.getData(), tenantId);
        deleteVisiDBDLTenantWithRetry(tenant);
    }

    public InstallResult deleteVisiDBDLTenantWithRetry(String tenant){
        DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenant, "3");
        InstallResult response = dataLoaderService.deleteDLTenant(request, dlUrl, true);
        return response;
    }

    public void verifyTenant(String tenant, String dlUrl, boolean expectToExists) {
        GetVisiDBDLRequest getRequest = new GetVisiDBDLRequest(tenant);
        if (expectToExists) {
            Assert.assertEquals(dataLoaderService.getDLTenantSettings(getRequest, dlUrl).getStatus(), 3);
        } else {
            Assert.assertEquals(dataLoaderService.getDLTenantSettings(getRequest, dlUrl).getStatus(), 5);
        }
    }

    public void verifyTenant(String tenant, String dlUrl) {
        verifyTenant(tenant, dlUrl, true);
    }

    @Override
    public String getServiceName() {
        return VisiDBDLComponent.componentName;
    }

}
