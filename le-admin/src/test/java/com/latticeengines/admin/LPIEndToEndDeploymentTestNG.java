package com.latticeengines.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminDeploymentTestNGBase;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.service.VboRequestLogService;
import com.latticeengines.admin.tenant.batonadapter.dante.DanteComponent;
import com.latticeengines.admin.tenant.batonadapter.datacloud.DataCloudComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponentDeploymentTestNG;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
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
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.domain.exposed.dcp.vbo.VboStatus;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.security.service.impl.IDaaSServiceImpl;

public class LPIEndToEndDeploymentTestNG extends AdminDeploymentTestNGBase {

    private static final String tenantName = "Global Test Tenant" + System.currentTimeMillis();
    private static final Logger log = LoggerFactory.getLogger(LPIEndToEndDeploymentTestNG.class);
    private static String tenantId = "EndToEnd";
    private static String contractId = "";
    private static String userEmail = "test@dcp2.com";

    private static final String HDFS_POD_PATH = "/Pods/%s/Contracts/%s";
    private static final String HDFS_MODELING_BASE_PATH = "/user/s-analytics/customers";

    @Value("{admin.default.subscription.number}")
    private static String subNumber;

    @Inject
    private TenantService tenantService;

    @Inject
    private com.latticeengines.security.exposed.service.TenantService secTenantService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ServiceService serviceService;

    @Inject
    private UserService userService;

    @Inject
    private IDaaSService iDaaSService;

    @Inject
    private PLSComponentDeploymentTestNG plsComponentDeploymentTestNG;

    @Inject
    private VboRequestLogService vboRequestLogService;

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
    public void setup() {
        tenantId = testContract + tenantId + System.currentTimeMillis();
        contractId = tenantId;

        loginAD();
        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        cleanup();
    }

    @Test(groups = "deployment", enabled = false)
    public void testEnd2End() throws Exception {
        provisionEndToEndTestTenants();
        log.info("Verify installation");
        verifyInstall();

        // uninstall
        log.info("Uninstall without wiping out ZK.");
        deleteTenant(contractId, tenantId, false);
        log.info("Verify uninstallation");
        verifyUninstall();

        // uninstall again
        log.info("Uninstall again with wiping out ZK.");
        deleteTenant(contractId, tenantId);
        //TODO: change this blind with to checking some condition
        Thread.sleep(10000);
        log.info("Uninstall again and again with wiping out ZK.");
        deleteTenant(contractId, tenantId);
    }

    @Test(groups = "deployment")
    public void testVboEnd2End() throws Exception {
        String fakeSubNumber = String.valueOf(System.currentTimeMillis());
        String traceId = provisionEndToEndVboTestTenants(fakeSubNumber);
        log.info("Verify installation");
        verifyZKState();
        verifyPLSTenantExists();
        verifyIDaasUserExists();
        verifyVboCallback(traceId);

        testExistingSubNumberViolation();

        log.info("Uninstall again with wiping out ZK.");
        deleteTenant(contractId, tenantId);
    }

    private void verifyIDaasUserExists() {
        IDaaSUser user = iDaaSService.getIDaaSUser(userEmail);
        Assert.assertNotNull(user);
        Assert.assertTrue(user.getApplications().contains(IDaaSServiceImpl.DCP_PRODUCT));
    }

    private VboRequest generateVBORequest(String currSubNumber) {
        VboRequest req = new VboRequest();
        VboRequest.Product pro = new VboRequest.Product();
        VboRequest.User user = new VboRequest.User();
        VboRequest.Name name = new VboRequest.Name();
        name.setFirstName("test2");
        name.setLastName("test2");
        user.setName(name);
        user.setUserId("testdcp2");
        user.setEmailAddress(userEmail);
        user.setTelephoneNumber("1234567");
        user.setPrimaryAddress(new VboRequest.PrimaryAddress());

        pro.setUsers(new ArrayList<>());
        pro.getUsers().add(user);
        req.setProduct(pro);
        VboRequest.Subscriber sub = new VboRequest.Subscriber();
        sub.setLanguage("English");
        sub.setName(tenantId);
        sub.setSubscriberNumber(currSubNumber);
        sub.setTenantType(TenantType.QA);
        req.setSubscriber(sub);
        return req;
    }

    private String provisionEndToEndVboTestTenants(String currSubNumber) {
        String url = getRestHostPort() + "/admin/tenants/vboadmin";
        VboRequest req = generateVBORequest(currSubNumber);

        VboResponse result = restTemplate.postForObject(url, req, VboResponse.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getStatus(), "success");

        return result.getAckReferenceId();
    }

    private void testExistingSubNumberViolation() {
        // test for vbo request
        VboRequest request = generateVBORequest(subNumber);
        VboResponse result = restTemplate.postForObject(getRestHostPort() + "/admin/tenants/vboadmin",
                request, VboResponse.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getStatus(), "failed");
    }

    // ==================================================
    // BEGIN: Verify main test tenant
    // ==================================================

    private void verifyInstall() {
        verifyZKState();
        verifyPLSMainTestTenantExists();
    }

    private void verifyUninstall() throws Exception {
        verifyZKUnistallState();
        //TODO: change this blind with to checking some condition
        Thread.sleep(10000);
        verifyHDFSFolderDeleted();
    }

    private void verifyPLSMainTestTenantExists() {
        verifyPLSTenantExists();
        verifyHDFSFolderExists();
    }

    // ==================================================
    // END: Verify main test tenant
    // ==================================================

    // ==================================================
    // BEGIN: Tenant creation methods
    // ==================================================

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
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties,
                "{\"Dante\":true,\"EnableDataEncryption\":true}");

        // SpaceConfiguration
        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();
        spaceConfiguration.setProducts(Collections.singletonList(LatticeProduct.LPA3));

        // PLS
        SerializableDocumentDirectory plsConfig = serviceService.getDefaultServiceConfig(PLSComponent.componentName);
        for (SerializableDocumentDirectory.Node node : plsConfig.getNodes()) {
            if (node.getNode().contains("SuperAdminEmails")) {
                node.setData("[\"ga_dev@lattice-engines.com\"]");
            } else if (node.getNode().contains("LatticeAdminEmails")) {
                node.setData("[]");
            }
        }
        plsConfig.setRootPath("/" + PLSComponent.componentName);

        // DataCloud
        SerializableDocumentDirectory dataCloudConfig = serviceService
                .getDefaultServiceConfig(DataCloudComponent.componentName);
        dataCloudConfig.setRootPath("/" + DataCloudComponent.componentName);

        // Combine configurations
        List<SerializableDocumentDirectory> configDirs = new ArrayList<>();
        configDirs.add(plsConfig);
        configDirs.add(dataCloudConfig);

        // Orchestrate tenant
        TenantRegistration reg = new TenantRegistration();
        reg.setContractInfo(new ContractInfo(new ContractProperties()));
        reg.setTenantInfo(tenantInfo);
        reg.setSpaceInfo(spaceInfo);
        reg.setSpaceConfig(spaceConfiguration);
        reg.setConfigDirectories(configDirs);

        String url = String.format("%s/admin/tenants/%s?contractId=%s", getRestHostPort(), tenantId, contractId);
        Boolean created = restTemplate.postForObject(url, reg, Boolean.class);
        Assert.assertNotNull(created);
        Assert.assertTrue(created);
    }

    // ==================================================
    // END: Tenant creation methods
    // ==================================================

    // ==================================================
    // BEGIN: Tenant verification methods
    // ==================================================
    private void verifyZKState() {
        ExecutorService executor = Executors.newFixedThreadPool(6);

        List<Future<BootstrapState>> futures = new ArrayList<>();
        List<String> serviceNames = new ArrayList<>(serviceService.getRegisteredServices());
        List<String> lp3ServiceNames = new ArrayList<>();
        for (String serviceName : serviceNames) {
            if (!(serviceName.toLowerCase().contains("test") || serviceName.equals(DanteComponent.componentName)
                    || (plsSkipped && serviceName.equals(PLSComponent.componentName)))) { lp3ServiceNames.add(serviceName);
            }
        }

        for (String serviceName : lp3ServiceNames) {
            final String component = serviceName;
            Future<BootstrapState> future = executor.submit(() -> //
                    waitUntilStateIsNotInitial(contractId, tenantId, component, 600));
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
            boolean thisIsOK = state != null && state.state.equals(BootstrapState.State.OK);
            if (!thisIsOK) {
                if (state != null) {
                    msg.append(String.format("The bootstrap state of %s is not OK, but rather %s : %s.\n", serviceName,
                            state.state, state.errorMessage));
                } else {
                    msg.append(String.format("The bootstrap state of %s is not OK, but rather null.\n", serviceName));
                }
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
            if (!(serviceName.toLowerCase().contains("test") || serviceName.equals(DanteComponent.componentName))) {
                lp3ServiceNames.add(serviceName);
            }
        }

        for (String serviceName : lp3ServiceNames) {
            final String component = serviceName;
            Future<BootstrapState> future = executor.submit(() -> //
                    waitUntilStateIsNotUninstalling(contractId, tenantId, component, 200));
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
                msg.append(String.format("The bootstrap state of %s is not UNINSTALLED, but rather %s : %s.\n",
                        serviceName, state.state, state.errorMessage));
            }
            allOK = allOK && thisIsOK;
        }
        Assert.assertTrue(allOK, msg.toString());
    }

    private void verifyPLSTenantExists() {
        if (plsSkipped) {
            return;
        }

        // check non-zero users
        final String PLSTenantId = String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        List<User> users = userService.getUsers(PLSTenantId);
        Assert.assertFalse(users.isEmpty());

        final String username = "ga_dev@lattice-engines.com";
        final String password = "WorkflowAp1";

        UserDocument userDoc = plsComponentDeploymentTestNG.loginAndAttach(username, password, PLSTenantId);
        Assert.assertNotNull(userDoc);
    }

    private void verifyHDFSFolderExists() {
        String modelingHdfsPoint = HDFS_MODELING_BASE_PATH + "/"
                + String.format("%s.%s.%s", contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        String podHdfsPoint = String.format(HDFS_POD_PATH, CamilleEnvironment.getPodId(), contractId);
        try {
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, modelingHdfsPoint), "modeling path not exist!");
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, podHdfsPoint), "Pod path not exist!");
        } catch (IOException e) {
            log.error("Error when verify the existence of hdfs paths", e);
        }
    }

    private void verifyHDFSFolderDeleted() {
        String modelingHdfsPoint = HDFS_MODELING_BASE_PATH + "/"
                + String.format("%s.%s.%s", contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        String podHdfsPoint = String.format(HDFS_POD_PATH, CamilleEnvironment.getPodId(), contractId);
        try {
            Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, modelingHdfsPoint),
                    "modeling path not deleted!");
            Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, podHdfsPoint), "Pod path not deleted!");
        } catch (IOException e) {
            log.error("Error when verify the existence of hdfs paths", e);
        }
    }

    private void verifyVboCallback(String traceId) throws Exception {
        VboRequestLog requestLog = null;
        for (int i = 0; i < 30 && (requestLog == null || requestLog.getCallbackRequest() == null); ++i) {
            Thread.sleep(60000);
            requestLog = vboRequestLogService.getVboRequestLogByTraceId(traceId);
            Assert.assertNotNull(requestLog);
        }
        Assert.assertNotNull(requestLog.getCallbackRequest());
        Assert.assertEquals(traceId, requestLog.getCallbackRequest().customerCreation.transactionDetail.ackRefId);
        Assert.assertEquals(requestLog.getCallbackRequest().customerCreation.transactionDetail.status, VboStatus.SUCCESS);
    }

    // ==================================================
    // NED: Tenant verification methods
    // ==================================================

    // ==================================================
    // BEGIN: Tenant cleanup methods
    // ==================================================
    private void cleanup() throws Exception {
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            log.error("clean up tenant error!");
        }
    }

    // ==================================================
    // END: Tenant cleanup methods
    // ==================================================
}
