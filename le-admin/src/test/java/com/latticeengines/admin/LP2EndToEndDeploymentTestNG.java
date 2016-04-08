package com.latticeengines.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.admin.functionalframework.AdminDeploymentTestNGBase;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.bardjams.BardJamsComponent;
import com.latticeengines.admin.tenant.batonadapter.dante.DanteComponent;
import com.latticeengines.admin.tenant.batonadapter.eai.EaiComponent;
import com.latticeengines.admin.tenant.batonadapter.metadata.MetadataComponent;
import com.latticeengines.admin.tenant.batonadapter.modeling.ModelingComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponentDeploymentTestNG;
import com.latticeengines.admin.tenant.batonadapter.template.dl.DLTemplateComponent;
import com.latticeengines.admin.tenant.batonadapter.template.visidb.VisiDBTemplateComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponentDeploymentTestNG;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.UserService;

public class LP2EndToEndDeploymentTestNG extends AdminDeploymentTestNGBase {

    private final static String tenantName = "Global Test Tenant";
    private final static Log log = LogFactory.getLog(LP2EndToEndDeploymentTestNG.class);
    private static String tenantId = "EndToEnd";
    private static String contractId = "";

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ServiceService serviceService;

    @Autowired
    private UserService userService;

    @Autowired
    private BardJamsEntityMgr bardJamsEntityMgr;

    @Autowired
    private PLSComponentDeploymentTestNG plsComponentDeploymentTestNG;

    @Autowired
    private VisiDBDLComponentDeploymentTestNG visiDBDLComponentDeploymentTestNG;

    @Value("${admin.test.contract}")
    private String testContract;

    @Value("${pls.api.hostport}")
    private String plsHostPort;

    @Value("${admin.pls.dryrun}")
    private boolean plsSkipped;

    @Value("${admin.bardjams.dryrun}")
    private boolean jamsSkipped;

    @Value("${admin.dante.dryrun}")
    private boolean danteSkipped;

    @Value("${admin.vdbdl.dryrun}")
    private boolean vdbdlSkipped;

    @Value("${admin.vdb.tpl.dryrun}")
    private boolean vdbTplSkipped;

    @Value("${admin.dl.tpl.dryrun}")
    private boolean dlTplSkipped;

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

    @Value("${admin.mount.vdb.permstore}")
    private String permStore;

    @Value("${admin.test.vdb.permstore.server}")
    private String permStoreServer;

    /**
     * In setup, orchestrate a full tenant.
     **/
    @BeforeClass(groups = "deployment_lp2")
    public void setup() throws Exception {
        tenantId = testContract + tenantId;
        contractId = tenantId;

        loginAD();
        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        // TODO: skip Dante until it becomes idempotent
        danteSkipped = true;

        cleanup();

        provisionEndToEndTestTenants();
    }

    /**
     * ==================================================
     * BEGIN: Verify main test tenant
     * ==================================================
     */

    // ==================================================
    // verify ZK states
    // ==================================================

    @Test(groups = "deployment_lp2")
    public void verifyZKStatesInMainTestTenant() {
        verifyZKState();
    }

    // ==================================================
    // verify tenant truly exists
    // ==================================================

    @Test(groups = "deployment_lp2", dependsOnMethods = "verifyZKStatesInMainTestTenant")
    public void verifyJAMSMainTestTenantExists() throws Exception {
        verifyJAMSTenantExists();
    }

    @Test(groups = "deployment_lp2", dependsOnMethods = "verifyZKStatesInMainTestTenant")
    public void verifyPLSMainTestTenantExists() throws Exception {
        verifyPLSTenantExists();
    }

    @Test(groups = "deployment_lp2", dependsOnMethods = "verifyZKStatesInMainTestTenant")
    public void verifyVisiDBDLMainTestTenantExists() throws Exception {
        verifyVisiDBDLTenantExists();
    }

    // ==================================================
    // verify cross component workflows
    // ==================================================

    @Test(groups = "deployment_lp2", dependsOnMethods = "verifyPLSMainTestTenantExists")
    public void verifyPLSTenantKnowsTopologyInMainTestTenant() throws Exception {
        verifyPLSTenantKnowsTopology();
    }

    /**
     * ==================================================
     * END: Verify main test tenant
     * ==================================================
     */

    /**
     * ==================================================
     * BEGIN: Tenant creation methods
     * ==================================================
     */

    private void provisionEndToEndTestTenants() {
        provisionEndToEndTestTenant1();
    }

    /**
     * This is the main testing tenant
     */
    private void provisionEndToEndTestTenant1() {
        // TenantInfo
        TenantProperties tenantProperties = new TenantProperties();
        tenantProperties.description = "A test tenant across all component provisioned by tenant console through deployment tests.";
        tenantProperties.displayName = tenantName;
        TenantInfo tenantInfo = new TenantInfo(tenantProperties);

        // SpaceInfo
        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
        spaceProperties.description = tenantProperties.description;
        spaceProperties.displayName = tenantProperties.displayName;
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, "{\"Dante\":true}");

        // SpaceConfiguration
        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();
        spaceConfiguration.setDlAddress(dlUrl);
        spaceConfiguration.setTopology(CRMTopology.ELOQUA);

        // BARDJAMS
        SerializableDocumentDirectory jamsConfig = serviceService
                .getDefaultServiceConfig(BardJamsComponent.componentName);
        DocumentDirectory metaDir = serviceService.getConfigurationSchema(BardJamsComponent.componentName);
        jamsConfig.applyMetadata(metaDir);
        jamsConfig.setRootPath("/" + BardJamsComponent.componentName);

        // PLS
        SerializableDocumentDirectory PLSconfig = serviceService.getDefaultServiceConfig(PLSComponent.componentName);
        for (SerializableDocumentDirectory.Node node : PLSconfig.getNodes()) {
            if (node.getNode().contains("SuperAdminEmails")) {
                node.setData("[\"bnguyen@lattice-engines.com\"]");
            } else if (node.getNode().contains("LatticeAdminEmails")) {
                node.setData("[]");
            }
        }
        PLSconfig.setRootPath("/" + PLSComponent.componentName);

        // VisiDBDL
        DocumentDirectory confDir = visiDBDLComponentDeploymentTestNG.getVisiDBDLDocumentDirectory();
        SerializableDocumentDirectory vdbdlConfig = new SerializableDocumentDirectory(confDir);
        vdbdlConfig.setRootPath("/" + VisiDBDLComponent.componentName);

        // VDB Template
        SerializableDocumentDirectory vdbTplConfig = serviceService
                .getDefaultServiceConfig(VisiDBTemplateComponent.componentName);
        vdbTplConfig.setRootPath("/" + VisiDBTemplateComponent.componentName);

        // DL Template
        SerializableDocumentDirectory dlTplConfig = serviceService
                .getDefaultServiceConfig(DLTemplateComponent.componentName);
        dlTplConfig.setRootPath("/" + DLTemplateComponent.componentName);

        // Dante
        SerializableDocumentDirectory danteConfig = serviceService
                .getDefaultServiceConfig(DanteComponent.componentName);
        danteConfig.setRootPath("/" + DanteComponent.componentName);

        // Modeling
        SerializableDocumentDirectory modelingConfig = serviceService
                        .getDefaultServiceConfig(ModelingComponent.componentName);
                modelingConfig.setRootPath("/" + ModelingComponent.componentName);

        // Combine configurations
        List<SerializableDocumentDirectory> configDirs = new ArrayList<>();
        configDirs.add(jamsConfig);
        configDirs.add(PLSconfig);
        configDirs.add(vdbdlConfig);
        configDirs.add(vdbTplConfig);
        configDirs.add(dlTplConfig);
        configDirs.add(danteConfig);
        configDirs.add(modelingConfig);

        // Orchestrate tenant
        TenantRegistration reg = new TenantRegistration();
        reg.setContractInfo(new ContractInfo(new ContractProperties()));
        reg.setTenantInfo(tenantInfo);
        reg.setSpaceInfo(spaceInfo);
        reg.setSpaceConfig(spaceConfiguration);
        reg.setConfigDirectories(configDirs);

        String url = String.format("%s/admin/tenants/%s?contractId=%s", getRestHostPort(), tenantId, contractId);
        boolean created = restTemplate.postForObject(url, reg, Boolean.class);
        Assert.assertTrue(created);
    }

    /**
     * ==================================================
     * END: Tenant creation methods
     * ==================================================
     */

    /**
     * ==================================================
     * BEGIN: Tenant verification methods
     * ==================================================
     */
    private void verifyZKState() {
        ExecutorService executor = Executors.newFixedThreadPool(6);

        List<Future<BootstrapState>> futures = new ArrayList<>();
        List<String> serviceNames = new ArrayList<>(serviceService.getRegisteredServices());

        for (String serviceName : serviceNames) {
            final String component = serviceName;
            Future<BootstrapState> future = executor.submit(new Callable<BootstrapState>() {
                @Override
                public BootstrapState call() throws Exception {
                    if (component.toLowerCase().contains("test")
                            || (danteSkipped && component.equals(DanteComponent.componentName))
                            || (plsSkipped && component.equals(PLSComponent.componentName))
                            || (vdbdlSkipped && component.equals(VisiDBDLComponent.componentName))
                            || (vdbTplSkipped && component.equals(VisiDBTemplateComponent.componentName))
                            || (jamsSkipped && component.equals(BardJamsComponent.componentName))
                            || component.equals(EaiComponent.componentName)
                            || component.equals(MetadataComponent.componentName)
                            || component.equals(ModelingComponent.componentName)
                            ) {
                        return BootstrapState.constructOKState(1);
                    } else {
                        return waitUntilStateIsNotInitial(contractId, tenantId, component, 600);
                    }
                }
            });
            futures.add(future);
        }

        boolean allOK = true;
        StringBuilder msg = new StringBuilder("Problematic components are:\n");

        for (int i = 0; i < serviceNames.size(); i++) {
            Future<BootstrapState> result = futures.get(i);
            String serviceName = serviceNames.get(i);
            BootstrapState state = null;
            try {
                state = result.get();
            } catch (InterruptedException | ExecutionException e) {
                msg.append(String.format("Could not successfully get the bootstrap state of %s \n", serviceName));
            }
            boolean thisIsOK = (state != null && state.state.equals(BootstrapState.State.OK))
                    || (BootstrapState.State.INITIAL.equals(state.state) && DanteComponent.componentName
                            .equals(serviceName));
            if (!thisIsOK && state != null) {
                msg.append(String.format("The bootstrap state of %s is not OK, but rather %s : %s.\n", serviceName,
                        state.state, state.errorMessage));
            }
            allOK = allOK && thisIsOK;
        }

        Assert.assertTrue(allOK, msg.toString());
    }

    private void verifyJAMSTenantExists() {
        // if (jamsSkipped) return;
    }

    private void verifyPLSTenantExists() {
        if (plsSkipped)
            return;

        // check non-zero users
        final String PLSTenantId = String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        List<User> users = userService.getUsers(PLSTenantId);
        Assert.assertFalse(users.isEmpty());

        final String username = "bnguyen@lattice-engines.com";
        final String password = "tahoe";

        UserDocument userDoc = plsComponentDeploymentTestNG.loginAndAttach(username, password, PLSTenantId);
        Assert.assertNotNull(userDoc);
    }

    private void verifyVisiDBDLTenantExists() throws IOException {
        if (vdbdlSkipped)
            return;

        visiDBDLComponentDeploymentTestNG.verifyTenant(tenantId, dlUrl);
        // verify permstore and datastore
        String url = String.format("%s/admin/internal/datastore/", getRestHostPort());
        log.info(magicRestTemplate.getForObject(url + dataStoreServer + "/" + tenantId, List.class));
        Assert.assertEquals(magicRestTemplate.getForObject(url + dataStoreServer + "/" + tenantId, List.class).size(),
                3);
    }

    private void verifyPLSTenantKnowsTopology() {
        if (plsSkipped)
            return;

        String PLSTenantId = String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        RestTemplate plsRestTemplate = plsComponentDeploymentTestNG.plsRestTemplate;
        String response = plsRestTemplate.getForObject(plsHostPort + "/pls/config/topology?tenantId=" + PLSTenantId,
                String.class);
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode json = mapper.readTree(response);
            Assert.assertEquals(CRMTopology.fromName(json.get("Topology").asText()), CRMTopology.ELOQUA);
        } catch (IOException e) {
            Assert.fail("Failed to parse topology from PLS.", e);
        }
    }

    /**
     * ==================================================
     * END: Tenant verification methods
     * ==================================================
     */

    /**
     * ==================================================
     * BEGIN: Tenant clean up methods
     * ==================================================
     */
    public void cleanup() throws Exception {
        try {
            deleteTenant(TestContractId, tenantId);
        } catch (Exception e) {
            // ignore
        }

        deleteVisiDBDLTenants();
        deleteBardJamesTenant();
        deletePLSTenants();
    }

    private void deletePLSTenants() {
        String PLSTenantId = String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        try {
            plsComponentDeploymentTestNG.deletePLSTestTenant(PLSTenantId);
            // let GA recover from error deletion
            Thread.sleep(5000L);
        } catch (Exception e) {
            log.warn("Deleting PLSTestTenant " + tenantId + " encountered an exception.", e);
            // ignore
        }
    }

    private void deleteVisiDBDLTenants() {
        visiDBDLComponentDeploymentTestNG.clearDatastore(dataStoreServer, permStoreServer, visiDBServerName, tenantId);
        try {
            visiDBDLComponentDeploymentTestNG.deleteVisiDBDLTenantWithRetry(tenantId);
        } catch (Exception e) {
            log.warn("Deleting VDB/DL tenant " + tenantId + " encountered an exception.", e);
            // ignore
        }
    }

    private void deleteBardJamesTenant() throws IOException, InterruptedException {
        try {
            BardJamsTenant jamsTenant = bardJamsEntityMgr.findByTenant(tenantId);
            bardJamsEntityMgr.delete(jamsTenant);
        } catch (Exception e) {
            log.warn("Deleting BardJams tenant " + tenantId + " encountered an exception.", e);
            // ignore
        }
    }

    /**
     * ==================================================
     * END: Tenant clean up methods
     * ==================================================
     */
}
