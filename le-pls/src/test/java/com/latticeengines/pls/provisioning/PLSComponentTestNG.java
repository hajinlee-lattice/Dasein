package com.latticeengines.pls.provisioning;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

public class PLSComponentTestNG extends PlsFunctionalTestNGBase {

    private static final BatonService batonService = new BatonServiceImpl();
    private static final String serviceName = "PLS";
    private static final String tenantName = "PLS Component Test Tenant";
    private static final String contractId = "PLSComponentTest";
    private static final String tenantId = "PLSComponentTest";
    private static final String spaceID = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;
    private static final String testAdminUsername = "pls-installer-tester@lattice-engines.com";

    @Autowired
    private PLSComponentManager componentManager;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Test(groups = "functional")
    public void testBootstrapTenant() throws InterruptedException {
        createTenantInZK();

        String PLSTenantId = String.format("%s.%s.%s", contractId, tenantId, spaceID);

        Tenant tenant = new Tenant();
        tenant.setId(PLSTenantId);
        tenant.setName(tenantName);

        try {
            userService.deleteUser(PLSTenantId, testAdminUsername);
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
            // ignore
        }

        bootstrap();

        // wait a while, then test your installation
        int numOfRetries = 10;
        BootstrapState state;
        do {
            state = batonService.getTenantServiceBootstrapState(contractId, tenantId, serviceName);
            numOfRetries--;
            Thread.sleep(1000L);
        } while (state.state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);

        if (!state.state.equals(BootstrapState.State.OK)) {
            Assert.fail(state.errorMessage);
        }

        Assert.assertTrue(tenantService.hasTenantId(tenant.getId()));

        Tenant newTenant = tenantService.findByTenantId(tenant.getId());
        Assert.assertEquals(newTenant.getName(), tenant.getName());

        AccessLevel level = userService.getAccessLevel(tenant.getId(), testAdminUsername);
        Assert.assertEquals(level, AccessLevel.SUPER_ADMIN);

        newTenant = tenantService.findByTenantId(tenant.getId());
        Assert.assertEquals(newTenant.getName(), tenantName);

        componentManager.discardTenant(tenant);
        Assert.assertFalse(tenantService.hasTenantId(tenant.getId()));
        Assert.assertFalse(userService.inTenant(tenant.getId(), testAdminUsername));

        cleanupZK();
    }

    private void createTenantInZK() {
        // use Baton to create a tenant in ZK
        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantProperties properties = new TenantProperties();
        properties.displayName = tenantName;
        TenantInfo tenantInfo = new TenantInfo(properties);
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(new CustomerSpaceProperties(), "");
        batonService.createTenant(contractId, tenantId, spaceID, contractInfo, tenantInfo, spaceInfo);
    }

    private void cleanupZK() {
        // use Baton to create a tenant in ZK
        batonService.deleteTenant(contractId, tenantId);
        try {
            if (ContractLifecycleManager.exists(contractId)) {
                ContractLifecycleManager.delete(contractId);
            }
        } catch (Exception e) {
            // ignore
        }

        try {
            Assert.assertFalse(ContractLifecycleManager.exists(contractId));
        } catch (Exception e) {
            Assert.fail("Cannot verify the deletion of contract " + contractId);
        }
    }

    private void bootstrap() {
        DocumentDirectory confDir = new DocumentDirectory(new Path("/"));
        confDir.add("/SuperAdminEmails", "[\"" + testAdminUsername + "\"]");
        confDir.add("/LatticeAdminEmails", "[ ]");
        confDir.add("/ExternalAdminEmails", "[ ]");
        batonService.bootstrap(contractId, tenantId, spaceID, serviceName,
                new SerializableDocumentDirectory(confDir).flatten());
    }

}
