package com.latticeengines.metadata.provisioning;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.service.TenantService;

public class MetadataComponentTestNG extends MetadataFunctionalTestNGBase {
    @Autowired
    private MetadataComponentManager componentManager;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private MetadataService mdService;

    private Tenant tenant;

    private static final BatonService batonService = new BatonServiceImpl();
    private static final String serviceName = "Metadata";
    private static final String tenantName = "Metadata Component Test Tenant";
    private static final String contractId = "MetadataComponentTest";
    private static final String tenantId = "MetadataComponentTest";
    private static final String spaceID = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;

    private String customerSpace = String.format("%s.%s.%s", contractId, tenantId, spaceID);

    @BeforeClass(groups = "functional")
    public void setup() {
        tenant = new Tenant();
        tenant.setId(customerSpace);
        tenant.setName("Metadata component manager test tenant");
        try {
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
        }
    }

    @Test(groups = { "functional" })
    public void testBootstrapTenant() throws Exception {
        createTenantInZK();
        tenantService.registerTenant(tenant);
        bootstrap();
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

        List<Table> tables = mdService.getImportTables(CustomerSpace.parse(customerSpace));
        assertEquals(tables.size(), 5);
        for (Table table : tables) {
            DateTime date = new DateTime(table.getLastModifiedKey().getLastModifiedTimestamp());
            assertTrue(date.plusYears(2).isBeforeNow());
        }
    }

    @AfterClass(groups = "functional")
    private void cleanUp() {
        tenantService.discardTenant(tenant);
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
        batonService.bootstrap(contractId, tenantId, spaceID, serviceName,
                new SerializableDocumentDirectory(confDir).flatten());
    }

}
