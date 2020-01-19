package com.latticeengines.metadata.provisioning;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.service.TenantService;

public class MetadataComponentManagerTestNG extends MetadataFunctionalTestNGBase {

    @Inject
    private MetadataComponentManager componentManager;

    @Inject
    private TenantService tenantService;

    @Inject
    private MetadataService mdService;

    private Tenant tenant;

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
        } catch (Exception ignore) {
            // tenant not exist
        }
    }

    @Test(groups = { "functional" })
    public void testProvisionTenant() {
        tenantService.registerTenant(tenant);
        componentManager.provisionImportTables(CustomerSpace.parse(customerSpace), new DocumentDirectory());
        List<Table> tables = mdService.getImportTables(CustomerSpace.parse(customerSpace));
        assertEquals(tables.size(), 5);
        for (Table table : tables) {
            DateTime date = new DateTime(table.getLastModifiedKey().getLastModifiedTimestamp());
            assertTrue(date.plusYears(2).isBeforeNow());
        }
    }

    @AfterClass(groups = "functional")
    public void cleanUp() {
        tenantService.discardTenant(tenant);
    }

}
