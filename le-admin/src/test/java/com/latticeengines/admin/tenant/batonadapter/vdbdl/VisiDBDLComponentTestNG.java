package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class VisiDBDLComponentTestNG extends BatonAdapterBaseDeploymentTestNG {

    @Autowired
    private TenantService tenantService;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    @Value("${admin.test.vbservername}")
    private String visiDBServerName;

    @Value("${admin.test.dl.user}")
    private String ownerEmail;

    private String tenant;

    private String tenantAlias;

    private String createNewVisiDB;

    private String visiDBName;

    @BeforeClass(groups = { "deployment", "functional" })
    @Override
    public void setup() throws Exception {
        super.setup();
        tenantAlias = tenantId;
        createNewVisiDB = "true";
        visiDBName = "TestVisiDB";
        tenant = tenantService.getTenant(contractId, tenantId).getTenantInfo().properties.displayName;
    }

    @Test(groups = "deployment")
    public void testInstallation() throws InterruptedException {
        Map<String, Object> response = deleteVisiDBDLTenant(tenant);
        Assert.assertEquals((Integer) response.get("Status"), new Integer(5));
        Assert.assertTrue(((String) response.get("ErrorMessage")).contains("does not exist"));

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/TenantAlias"));
        node.getDocument().setData(tenantAlias);
        node = confDir.get(new Path("/DLUrl"));
        node.getDocument().setData(dlUrl);
        node = confDir.get(new Path("/VisiDB"));
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
        Assert.assertEquals((Integer) response.get("Status"), new Integer(3));

    }

    @Test(groups = "functional")
    public void testInstallationFunctional() throws InterruptedException {

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/TenantAlias"));
        node.getDocument().setData(tenantAlias);
        node = confDir.get(new Path("/DLUrl"));
        node.getDocument().setData(dlUrl);
        node = confDir.get(new Path("/VisiDB"));
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
                Assert.assertEquals(sNode.getData(), tenantAlias);
            }
        }

    }

    private Map<String, Object> deleteVisiDBDLTenant(String tenant) {
        try {
            DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenant, "3");
            String jsonStr = JsonUtils.serialize(request);
            List<BasicNameValuePair> headers = new ArrayList<>();
            headers.add(new BasicNameValuePair("MagicAuthentication", "Security through obscurity!"));
            headers.add(new BasicNameValuePair("Content-Type", "application/json"));
            headers.add(new BasicNameValuePair("Accept", "application/json"));
            String response = HttpClientWithOptionalRetryUtils.sendPostRequest("http://10.41.1.207:8081"
                    + "/DLRestService/DeleteDLTenant", false, headers, jsonStr);
            return convertToMap(response);
        } catch (Exception e) {
            Assert.fail("could not encode the username");
        }
        return null;
    }

    private Map<String, Object> convertToMap(String response) {
        Map<String, Object> map = new HashMap<>();
        try {
            map = new ObjectMapper().readValue(response, Map.class);
        } catch (Exception e) {
            Assert.fail("could not read the response to map");
        }
        return map;
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
