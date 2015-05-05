package com.latticeengines.admin.tenant.batonadapter.pls;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

public class PLSComponentDeploymentTestNG extends PLSComponentTestNG {

    @Test(groups = "deployment", dependsOnMethods = "testInstallation")
    public void installTestTenants() {
        createTestTenant("Tenant1", "Tenant 1");
        createTestTenant("Tenant2", "Tenant 2");
    }

    private void createTestTenant(String tenantId, String tenantName) {
        loginAD();

        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
        createTenant(contractId, tenantId);

        String testAdminUsername = "bnguyen@lattice-engines.com";
        String testAdminPassword = "admin";

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/AdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        node = confDir.get(new Path("/TenantName"));
        node.getDocument().setData(TestContractId + " " + tenantName);

        // send to bootstrapper message queue
        super.bootstrap(contractId, tenantId, serviceName, confDir);
    }

}
