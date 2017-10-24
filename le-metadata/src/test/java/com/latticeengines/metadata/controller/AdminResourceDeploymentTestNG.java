package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class AdminResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AdminResourceDeploymentTestNG.class);

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
    protected TenantService tenantService;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();
    }

    @Test(groups = "deployment")
    public void testProvisionImportTables() {
        tenantService.discardTenant(tenant1);
        tenantService.registerTenant(tenant1);
        metadataProxy.provisionImportTables(tenant1);
        List<Table> tables = metadataProxy.getImportTables(customerSpace1);
        assertEquals(tables.size(), 5);
        for (Table table : tables) {
            DateTime date = new DateTime(table.getLastModifiedKey().getLastModifiedTimestamp());
            assertTrue(date.plusYears(2).isBeforeNow());
        }
    }
}
