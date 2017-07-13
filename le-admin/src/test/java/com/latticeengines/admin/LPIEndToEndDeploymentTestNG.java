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

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.HdfsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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

public class LPIEndToEndDeploymentTestNG extends AdminDeploymentTestNGBase {

    private final static String tenantName = "Global Test Tenant" + System.currentTimeMillis();
    private final static Logger log = LoggerFactory.getLogger(LPIEndToEndDeploymentTestNG.class);
    private static String tenantId = "EndToEnd";
    private static String contractId = "";

    private static final String HDFS_POD_PATH = "/Pods/%s/Contracts/%s";
    private static final String HDFS_MODELING_BASE_PATH = "/user/s-analytics/customers";

    @Autowired
    private TenantService tenantService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ServiceService serviceService;

    @Autowired
    private UserService userService;


    @Autowired
    private PLSComponentDeploymentTestNG plsComponentDeploymentTestNG;

    @Value("${admin.test.contract}")
    private String testContract;

    @Value("${common.test.pls.url}")
    private String plsHostPort;

    @Value("${admin.pls.dryrun}")
    private boolean plsSkipped;

    /**
     * In setup, orchestrateForInstall a full tenant.
     **/
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        tenantId = testContract + tenantId + System.currentTimeMillis();
        contractId = tenantId;

        loginAD();
        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        provisionEndToEndTestTenants();
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        cleanup();
    }

    /**
     * ================================================== BEGIN: Verify main
     * test tenant ==================================================
     */

    // ==================================================
    // verify ZK states
    // ==================================================

    @Test(groups = "deployment")
    public void verifyZKStatesInMainTestTenant() {
        verifyZKState();
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyZKStatesInMainTestTenant")
    public void verifyPLSMainTestTenantExists() throws Exception {
        verifyPLSTenantExists();
        verifyHDFSFolderExists();
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyPLSMainTestTenantExists")
    public void verifyPLSTenantKnowsTopologyInMainTestTenant() throws Exception {
        verifyPLSTenantKnowsTopology();
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyPLSTenantKnowsTopologyInMainTestTenant", enabled = true)
    public void uninstallComponents() throws Exception {
        deleteTenant(contractId, tenantId, false);
    }

    @Test(groups = "deployment", dependsOnMethods = "uninstallComponents", enabled = true)
    public void verifyUninstallStatus() throws Exception {
        verifyZKUnistallState();
        Thread.sleep(10000);
        verifyHDFSFolderDeleted();
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyUninstallStatus", enabled = true)
    public void uninstallComponentsAgain() throws Exception {
        deleteTenant(contractId, tenantId);
    }

    @Test(groups = "deployment", dependsOnMethods = "uninstallComponentsAgain", enabled = true)
    public void uninstallComponentsAfterSomeTime() throws Exception {
        Thread.sleep(10000);
        deleteTenant(contractId, tenantId);
    }

    /**
     * ================================================== END: Verify main test
     * tenant ==================================================
     */

    /**
     * ================================================== BEGIN: Tenant creation
     * methods ================`==================================
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
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, "{\"Dante\":true,\"EnableDataEncryption\":true}");

        // SpaceConfiguration
        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();
        //spaceConfiguration.setDlAddress(dlUrl);
        spaceConfiguration.setTopology(CRMTopology.ELOQUA);

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


        // Modeling
        SerializableDocumentDirectory modelingConfig = serviceService
                .getDefaultServiceConfig(ModelingComponent.componentName);
        modelingConfig.setRootPath("/" + ModelingComponent.componentName);

        // Combine configurations
        List<SerializableDocumentDirectory> configDirs = new ArrayList<>();
        configDirs.add(PLSconfig);
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
     * ================================================== END: Tenant creation
     * methods ==================================================
     */

    /**
     * ================================================== BEGIN: Tenant
     * verification methods ==================================================
     */
    private void verifyZKState() {
        ExecutorService executor = Executors.newFixedThreadPool(6);

        List<Future<BootstrapState>> futures = new ArrayList<>();
        List<String> serviceNames = new ArrayList<>(serviceService.getRegisteredServices());
        List<String> lp3ServiceNames = new ArrayList<>();
        for (String serviceName : serviceNames) {
            if (serviceName.toLowerCase().contains("test")
                    || serviceName.equals(DanteComponent.componentName)
                    || (plsSkipped && serviceName.equals(PLSComponent.componentName))
                    || serviceName.equals(VisiDBDLComponent.componentName)
                    || serviceName.equals(VisiDBTemplateComponent.componentName)
                    || serviceName.equals(DLTemplateComponent.componentName)
                    || serviceName.equals(BardJamsComponent.componentName)
                    || serviceName.equals(EaiComponent.componentName)
                    || serviceName.equals(MetadataComponent.componentName)) {
                continue;
            } else {
                lp3ServiceNames.add(serviceName);
            }
        }

        for (String serviceName : lp3ServiceNames) {
            final String component = serviceName;
            Future<BootstrapState> future = executor.submit(new Callable<BootstrapState>() {
                @Override
                public BootstrapState call() throws Exception {
                    return waitUntilStateIsNotInitial(contractId, tenantId, component, 600);
                }
            });
            futures.add(future);
        }

        boolean allOK = true;
        StringBuilder msg = new StringBuilder("Problematic components are:\n");

        for (int i = 0; i < lp3ServiceNames.size(); i++) {
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

    private void verifyZKUnistallState() {
        ExecutorService executor = Executors.newFixedThreadPool(6);

        List<Future<BootstrapState>> futures = new ArrayList<>();
        List<String> serviceNames = new ArrayList<>(serviceService.getRegisteredServices());
        List<String> lp3ServiceNames = new ArrayList<>();
        for (String serviceName : serviceNames) {
            if (serviceName.toLowerCase().contains("test")
                    || serviceName.equals(DanteComponent.componentName)
                    || serviceName.equals(VisiDBDLComponent.componentName)
                    || serviceName.equals(VisiDBTemplateComponent.componentName)
                    || serviceName.equals(DLTemplateComponent.componentName)
                    || serviceName.equals(BardJamsComponent.componentName)
                    || serviceName.equals(EaiComponent.componentName)
                    || serviceName.equals(MetadataComponent.componentName)
                    || serviceName.equals(ModelingComponent.componentName)) {
                continue;
            } else {
                lp3ServiceNames.add(serviceName);
            }
        }

        for (String serviceName : lp3ServiceNames) {
            final String component = serviceName;
            Future<BootstrapState> future = executor.submit(new Callable<BootstrapState>() {
                @Override
                public BootstrapState call() throws Exception {
                return waitUntilStateIsNotUninstalling(contractId, tenantId, component, 200);
                }
            });
            futures.add(future);
        }

        boolean allOK = true;
        StringBuilder msg = new StringBuilder("Problematic components are:\n");

        for (int i = 0; i < lp3ServiceNames.size(); i++) {
            Future<BootstrapState> result = futures.get(i);
            String serviceName = serviceNames.get(i);
            BootstrapState state = null;
            try {
                state = result.get();
            } catch (InterruptedException | ExecutionException e) {
                msg.append(String.format("Could not successfully get the bootstrap state of %s \n", serviceName));
            }
            boolean thisIsOK = (state != null && state.state.equals(BootstrapState.State.UNINSTALLED));
            if (!thisIsOK && state != null) {
                msg.append(String.format("The bootstrap state of %s is not UNINSTALLED, but rather %s : %s.\n", serviceName,
                        state.state, state.errorMessage));
            }
            allOK = allOK && thisIsOK;
        }
        Assert.assertTrue(allOK, msg.toString());
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

    private void verifyHDFSFolderExists() {
        String modelingHdfsPoint = HDFS_MODELING_BASE_PATH + "/" + String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        String podHdfsPoint = String.format(HDFS_POD_PATH, CamilleEnvironment.getPodId(), contractId);
        try {
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, modelingHdfsPoint),
                    "modeling path not exist!");
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, podHdfsPoint), "Pod path not exist!");
        } catch (IOException e) {

        }
    }

    private void verifyHDFSFolderDeleted() {
        String modelingHdfsPoint = HDFS_MODELING_BASE_PATH + "/" + String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        String podHdfsPoint = String.format(HDFS_POD_PATH, CamilleEnvironment.getPodId(), contractId);
        try {
            Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, modelingHdfsPoint),
                    "modeling path not deleted!");
            Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, podHdfsPoint), "Pod path not deleted!");
        } catch (IOException e) {
            e.printStackTrace();
        }
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
     * ================================================== END: Tenant
     * verification methods ==================================================
     */

    /**
     * ================================================== BEGIN: Tenant clean up
     * methods ==================================================
     */
    public void cleanup() throws Exception {
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            log.error("clean up tenant error!");
        }
    }

    /**
     * ================================================== END: Tenant clean up
     * methods ==================================================
     */
}
