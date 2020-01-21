package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataProvisioningService;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class MetadataProvisioningServiceImplTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MetadataProvisioningServiceImplTestNG.class);

    @Inject
    private MetadataProvisioningService metadataProvisioningService;

    @Inject
    private TenantService tenantService;

    private Tenant tenant;

    private CustomerSpace space;

    @Inject
    private MetadataService mdService;

    @BeforeClass(groups = "functional")
    public void setup() {
        space = CustomerSpace.parse(this.getClass().getSimpleName());
        tenant = new Tenant();
        tenant.setId(space.toString());
        tenant.setName("Metadata provisioning service test tenant");
        try {
            tenantService.discardTenant(tenant);
        } catch (Exception ignore) {
            // tenant not exist
        }
    }

    @Test(groups = { "functional" })
    public void provisionImportTables() {
        tenantService.registerTenant(tenant);
        metadataProvisioningService.provisionImportTables(space);
        List<Table> tables = mdService.getImportTables(space);
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

    void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    CustomerSpace getCustomerSpace() {
        return space;
    }
}
