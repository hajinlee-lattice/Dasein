package com.latticeengines.admin.tenant.batonadapter.pls;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;

public class PLSComponentDeploymentTestNG extends PLSComponentTestNG {

    @Test(groups = "deployment", dependsOnMethods = "testInstallation")
    public void installTestTenants() throws Exception {
        createTestTenant("Tenant1", "Tenant 1", "MARKETO");
        createTestTenant("Tenant2", "Tenant 2", "ELOQUA");
        createCommonTenant();
    }

    private void createTestTenant(String tenantId, String tenantName, String topology)
    throws Exception{
        loginAD();

        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "PLS Test tenant";
        props.displayName = TestContractId + " " + tenantName;
        props.topology = topology;
        props.product = "LPA";
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(props, "");

        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(
                new TenantProperties(spaceInfo.properties.displayName, spaceInfo.properties.description));

        TenantRegistration reg = new TenantRegistration();
        reg.setSpaceInfo(spaceInfo);
        reg.setTenantInfo(tenantInfo);
        reg.setContractInfo(contractInfo);

        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
        createTenant(contractId, tenantId, false, reg);

        String testAdminUsername = "bnguyen@lattice-engines.com";

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

    private void createCommonTenant() throws Exception {

        String contractId = "CommonTestContract";
        String tenantId = "TestTenant";
        loginAD();

        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "PLS Test tenant";
        props.displayName = "Lattice Internal Test Tenant";
        props.topology = "MARKETO";
        props.product = "LPA";
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(props, "");

        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(
                new TenantProperties(spaceInfo.properties.displayName, spaceInfo.properties.description));

        TenantRegistration reg = new TenantRegistration();
        reg.setSpaceInfo(spaceInfo);
        reg.setTenantInfo(tenantInfo);
        reg.setContractInfo(contractInfo);

        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
        createTenant(contractId, tenantId, false, reg);

        String testAdminUsername = "bnguyen@lattice-engines.com";

        DocumentDirectory confDir = batonService.getDefaultConfiguration(getServiceName());
        confDir.makePathsLocal();

        // modify the default config
        DocumentDirectory.Node node = confDir.get(new Path("/AdminEmails"));
        node.getDocument().setData("[\"" + testAdminUsername + "\"]");

        node = confDir.get(new Path("/TenantName"));
        node.getDocument().setData("Lattice Internal Test Tenant");

        // send to bootstrapper message queue
        super.bootstrap(contractId, tenantId, serviceName, confDir);
    }

}
