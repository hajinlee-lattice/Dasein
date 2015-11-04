package com.latticeengines.metadata.provisioning;

import java.util.List;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.service.TenantService;

public class MetadataComponentManagerTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private MetadataComponentManager componentManager;

    @Autowired
    private TenantService tenantService;

    @Autowired
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
        } catch (Exception e) {
        }
    }

    @Test(groups = { "functional" })
    public void testProvisionTenant() throws Exception {
        tenantService.registerTenant(tenant);
        componentManager.provisionTenant(CustomerSpace.parse(customerSpace), new DocumentDirectory());
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
    }

}
