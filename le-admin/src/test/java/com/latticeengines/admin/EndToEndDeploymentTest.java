package com.latticeengines.admin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import com.latticeengines.admin.tenant.batonadapter.dante.DanteComponent;
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

public class EndToEndDeploymentTest extends AdminFunctionalTestNGBase {

    private final static String contractId = "EndToEndTestContract";
    private final static String[] tenantIds = new String[]{"EndToEndTenant", "EndToEndDefaultTenant"};

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
        tenantProperties.displayName = "Global Test Tenant 1";
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
        tenantProperties.displayName = "Global Test Tenant Default";
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

        tenantService.createTenant(contractId, tenantId, null);
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
    private void verifyJAMSTenantExists(int tenantIdx) {
        if (jamsSkipped) return;

        String tenantId = tenantIds[tenantIdx];
        // check state in ZK
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, BardJamsComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
    }

    private void verifyPLSTenantExists(int tenantIdx) {
        if (plsSkipped) return;

        String tenantId = tenantIds[tenantIdx];
        // check state in ZK
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK);

        // check non-zero users
        String PLSTenantId =
                String.format("%s.%s.%s", contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        List<User> users  = userService.getUsers(PLSTenantId);
        Assert.assertFalse(users.isEmpty());
    }

    private void verifyVisiDBDLTenantExists(int tenantIdx) {
        if (vdbdlSkipped) return;

        String tenantId = tenantIds[tenantIdx];
        // check state in ZK
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, VisiDBDLComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
    }

    private void verifyVDBTplTenantExists(int tenantIdx) {
        if (vdbTplSkipped) return;

        String tenantId = tenantIds[tenantIdx];
        // check state in ZK
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, VisiDBTemplateComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
    }

    private void verifyDLTplTenantExists(int tenantIdx) {
        if (dlTplSkipped) return;

        String tenantId = tenantIds[tenantIdx];
        // check state in ZK
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, DLTemplateComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
    }

    @SuppressWarnings("unused")
    private void verifyDanteTenantExists(int tenantIdx) {
        if (danteSkipped) return;

        String tenantId = tenantIds[tenantIdx];
        // check state in ZK
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, DanteComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK);
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
