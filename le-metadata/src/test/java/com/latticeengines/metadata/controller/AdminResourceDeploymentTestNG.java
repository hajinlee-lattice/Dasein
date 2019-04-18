package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AdminResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {

    @Autowired
    protected TenantService tenantService;

    // TODO: disabled for M19 release, can try turn back later
    @Test(groups = "deployment", enabled = false)
    public void testProvisionImportTables() {
        String tenantName = TestFrameworkUtils.generateTenantName();
        String tenantId = CustomerSpace.parse(tenantName).toString();
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(tenantName);
        tenantService.discardTenant(tenant);
        tenantService.registerTenant(tenant);
        metadataProxy.provisionImportTables(tenant);
        List<Table> tables = metadataProxy.getImportTables(tenantId);
        assertEquals(tables.size(), 5);
        for (Table table : tables) {
            DateTime date = new DateTime(table.getLastModifiedKey().getLastModifiedTimestamp());
            assertTrue(date.plusYears(2).isBeforeNow());
        }
    }
}
