package com.latticeengines.admin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.bardjams.BardJamsComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.admin.tenant.batonadapter.template.dl.DLTemplateComponent;
import com.latticeengines.admin.tenant.batonadapter.template.visidb.VisiDBTemplateComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
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
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.UserService;

public class EndToEndDeploymentTestNG extends AdminFunctionalTestNGBase {

    private final static String contractId = "EndToEndTestContract";
    private final static String[] tenantIds = new String[]{"EndToEndTenant", "EndToEndDefaultTenant"};
    private final static String[] tenantNames =
            new String[]{"Global Test Tenant", "Global Test Tenant Default"};

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ServiceService serviceService;

    @Autowired
    private UserService userService;

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

    /**
     * In setup, orchestrate 2 full tenant.
     * The first one is the main testing tenant.
     * The second one uses only default configuration, to make sure default configuration can work out of box.
     *
     * @throws Exception
     */
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        loginAD();

        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));

        for (String tenantId: tenantIds) {
            try {
                deleteTenant(TestContractId, tenantId);
            } catch (Exception e) {
                //ignore
            }
        }
        // delete PLS tenant
        deletePLSTenants();

        provisionEndToEndTestTenants();
    }

    /**
     * Delete the two testing tenant in both ZK and each component.
     *
     * @throws Exception
     */
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        for (String tenantId: tenantIds) {
            try {
                deleteTenant(TestContractId, tenantId);
            } catch (Exception e) {
                // ignore
            }
        }

        // delete PLS tenant
        deletePLSTenants();
    }

    @Test(groups = "deployment")
    public void verifyMainTestTenant() throws Exception {
        // verify exsistence of each component
        verifyZKState(0);
        verifyJAMSTenantExists(0);
        verifyPLSTenantExists(0);
        verifyVisiDBDLTenantExists(0);
        verifyVDBTplTenantExists(0);
        verifyDLTplTenantExists(0);
        //verifyDanteTenantExists(0);

        // verify important cross-component work flows

    }

    @Test(groups = "deployment")
    public void verifyDefaultTestTenant() throws Exception {
        // verify exsistence of each component
        verifyZKState(1);
        verifyJAMSTenantExists(1);
        verifyPLSTenantExists(1);
        verifyVisiDBDLTenantExists(1);
        verifyVDBTplTenantExists(1);
        verifyDLTplTenantExists(1);
        //verifyDanteTenantExists(1);

        // verify minimal cross-component work flows

    }

    private void provisionEndToEndTestTenants() {
        provisionEndToEndTestTenant1();
        provisionEndToEndTestTenant2();
    }

    /**
     * ==================================================
     * BEGIN: Tenants creation methods
     * ==================================================
     */
    /**
     * This is the main testing tenant
     */
    private void provisionEndToEndTestTenant1() {
        String tenantId = tenantIds[0];

        // TenantInfo
        TenantProperties tenantProperties = new TenantProperties();
        tenantProperties.description =
                "First test tenant across all component provisioned by tenant console through deployment tests.";
        tenantProperties.displayName = tenantNames[0];
        TenantInfo tenantInfo = new TenantInfo(tenantProperties);

        // SpaceInfo
        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
        spaceProperties.description = tenantProperties.description;
        spaceProperties.displayName = tenantProperties.displayName;
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, "{\"Dante\":true}");

        // SpaceConfiguration
        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();

        // BARDJAMS
        SerializableDocumentDirectory jamsConfig =
                serviceService.getDefaultServiceConfig(BardJamsComponent.componentName);
        jamsConfig.setRootPath("/" + BardJamsComponent.componentName);

        // PLS
        SerializableDocumentDirectory PLSconfig = serviceService.getDefaultServiceConfig(PLSComponent.componentName);
        for (SerializableDocumentDirectory.Node node: PLSconfig.getNodes()) {
            if (node.getNode().contains("SuperAdminEmails")) {
                node.setData("[\"bnguyen@lattice-engines.com\"]");
            } else if (node.getNode().contains("LatticeAdminEmails")) {
                node.setData("[]");
            }
        }
        PLSconfig.setRootPath("/" + PLSComponent.componentName);

        // VisiDBDL
        SerializableDocumentDirectory vdbdlConfig =
                serviceService.getDefaultServiceConfig(VisiDBDLComponent.componentName);
        vdbdlConfig.setRootPath("/" + VisiDBDLComponent.componentName);

        // VDB Template
        SerializableDocumentDirectory vdbTplConfig =
                serviceService.getDefaultServiceConfig(VisiDBTemplateComponent.componentName);
        vdbTplConfig.setRootPath("/" + VisiDBTemplateComponent.componentName);

        // DL Template
        SerializableDocumentDirectory dlTplConfig =
                serviceService.getDefaultServiceConfig(DLTemplateComponent.componentName);
        dlTplConfig.setRootPath("/" + DLTemplateComponent.componentName);

        // Combine configurations
        List<SerializableDocumentDirectory> configDirs = new ArrayList<>();
        configDirs.add(jamsConfig);
        configDirs.add(PLSconfig);
        configDirs.add(vdbdlConfig);
        configDirs.add(vdbTplConfig);
        configDirs.add(dlTplConfig);

        // Orchestrate tenant
        TenantRegistration reg =  new TenantRegistration();
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
     * This is the tenant with default configuration
     */
    private void provisionEndToEndTestTenant2() {
        String tenantId = tenantIds[1];

        // TenantInfo
        TenantProperties tenantProperties = new TenantProperties();
        tenantProperties.description = "A cross-component tenant with default configurations.";
        tenantProperties.displayName = tenantNames[1];
        TenantInfo tenantInfo = new TenantInfo(tenantProperties);

        // SpaceInfo
        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
        spaceProperties.description = tenantProperties.description;
        spaceProperties.displayName = tenantProperties.displayName;
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, "");

        // SpaceConfiguration
        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();

        // BARDJAMS
        SerializableDocumentDirectory jamsConfig =
                serviceService.getDefaultServiceConfig(BardJamsComponent.componentName);
        jamsConfig.setRootPath("/" + BardJamsComponent.componentName);

        // PLS
        SerializableDocumentDirectory PLSconfig = serviceService.getDefaultServiceConfig(PLSComponent.componentName);
        PLSconfig.setRootPath("/" + PLSComponent.componentName);

        // VisiDBDL
        SerializableDocumentDirectory vdbdlConfig =
                serviceService.getDefaultServiceConfig(VisiDBDLComponent.componentName);
        vdbdlConfig.setRootPath("/" + VisiDBDLComponent.componentName);

        // VDB Template
        SerializableDocumentDirectory vdbTplConfig =
                serviceService.getDefaultServiceConfig(VisiDBTemplateComponent.componentName);
        vdbTplConfig.setRootPath("/" + VisiDBTemplateComponent.componentName);

        // DL Template
        SerializableDocumentDirectory dlTplConfig =
                serviceService.getDefaultServiceConfig(DLTemplateComponent.componentName);
        dlTplConfig.setRootPath("/" + DLTemplateComponent.componentName);

        // Combine configurations
        List<SerializableDocumentDirectory> configDirs = new ArrayList<>();
        configDirs.add(jamsConfig);
        configDirs.add(PLSconfig);
        configDirs.add(vdbdlConfig);
        configDirs.add(vdbTplConfig);
        configDirs.add(dlTplConfig);

        // Orchestrate tenant
        TenantRegistration reg =  new TenantRegistration();
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
     * END: Tenants creation methods
     * ==================================================
     */

    /**
     * ==================================================
     * BEGIN: Tenants verification methods
     * ==================================================
     */
    private void verifyZKState(int tenantIdx) {
        final String tenantId = tenantIds[tenantIdx];

        ExecutorService executor = Executors.newFixedThreadPool(6);

        List<Future<BootstrapState>> futures = new ArrayList<>();
        List<String> serviceNames = new ArrayList<>(serviceService.getRegisteredServices());

        for(String serviceName: serviceNames) {
            final String component = serviceName;
            Future<BootstrapState> future = executor.submit(new Callable<BootstrapState>() {
                @Override
                public BootstrapState call() throws Exception {
                    // not ready for integration test with Dante
                    if (component.toLowerCase().contains("dante") ||
                            component.toLowerCase().contains("test")) {
                        return BootstrapState.constructOKState(1);
                    } else {
                        return waitUntilStateIsNotInitial(contractId, tenantId, component);
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
            } catch (InterruptedException|ExecutionException e) {
                msg.append(String.format(
                        "Could not successfully get the bootstrap state of %s \n", serviceName));
            }
            boolean thisIsOK = state != null && state.state.equals(BootstrapState.State.OK);
            if (!thisIsOK && state != null) {
                msg.append(String.format(
                        "The bootstrap state of %s is not OK, but rather %s.\n", serviceName, state.state));
            }
            allOK = allOK && thisIsOK;
        }

        Assert.assertTrue(allOK, msg.toString());
    }

    @SuppressWarnings("unused")
    private void verifyJAMSTenantExists(int tenantIdx) {
        if (jamsSkipped) return;

        String tenantId = tenantIds[tenantIdx];
    }

    @SuppressWarnings("unused")
    private void verifyPLSTenantExists(int tenantIdx) {
        if (plsSkipped) return;

        final String tenantId = tenantIds[tenantIdx];

        // check non-zero users
        String PLSTenantId =
                String.format("%s.%s.%s", contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        List<User> users  = userService.getUsers(PLSTenantId);
        Assert.assertFalse(users.isEmpty());
    }

    @SuppressWarnings("unused")
    private void verifyVisiDBDLTenantExists(int tenantIdx) {
        if (vdbdlSkipped) return;

        final String tenantId = tenantIds[tenantIdx];
    }

    @SuppressWarnings("unused")
    private void verifyVDBTplTenantExists(int tenantIdx) {
        if (vdbTplSkipped) return;

        final String tenantId = tenantIds[tenantIdx];
    }

    @SuppressWarnings("unused")
    private void verifyDLTplTenantExists(int tenantIdx) {
        if (dlTplSkipped) return;

        final String tenantId = tenantIds[tenantIdx];
    }

    @SuppressWarnings("unused")
    private void verifyDanteTenantExists(int tenantIdx) {
        if (danteSkipped) return;

        final String tenantId = tenantIds[tenantIdx];
    }
    /**
     * ==================================================
     * END: Tenants verification methods
     * ==================================================
     */

    /**
     * ==================================================
     * BEGIN: Tenants clean up methods
     * ==================================================
     */
    private void deletePLSTenants() {
        for (String tenantId: tenantIds) {
            String PLSTenantId = String.format("%s.%s.%s",
                    contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
            try {
                magicRestTemplate.delete(plsHostPort + String.format("/pls/admin/tenants/%s", PLSTenantId));
            } catch (Exception e) {
                // ignore
            }
        }
    }
    /**
     * ==================================================
     * END: Tenants clean up methods
     * ==================================================
     */
}
