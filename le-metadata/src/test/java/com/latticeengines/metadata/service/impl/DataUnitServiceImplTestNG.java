package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataUnitService;

public class DataUnitServiceImplTestNG extends MetadataFunctionalTestNGBase {

    @Inject
    private DataUnitService dataUnitService;

    private String testTenantId;

    @BeforeClass(groups = "functional")
    public void setup() {
        functionalTestBed.bootstrap(1);
        Tenant testTenant = functionalTestBed.getMainTestTenant();
        MultiTenantContext.setTenant(testTenant);
        String customerSpace = CustomerSpace.parse(testTenant.getId()).toString();
        testTenantId = CustomerSpace.shortenCustomerSpace(customerSpace);

    }

    @Test(groups = "functional")
    public void testCrud() throws Exception {
        String name = NamingUtils.timestamp("Dynamo");
        DataUnit unit = dataUnitService.createOrUpdateByName(createDynamoUnit(name));
        Assert.assertNotNull(unit);
        Assert.assertTrue(unit instanceof DynamoDataUnit);

        Thread.sleep(500);
        DataUnit found = dataUnitService.findByNameFromReader(name);
        Assert.assertNotNull(found);
        Assert.assertTrue(found instanceof DynamoDataUnit);

        dataUnitService.deleteByName(name);
        Thread.sleep(500);
        Assert.assertNull(dataUnitService.findByNameFromReader(name));
    }

    private DynamoDataUnit createDynamoUnit(String name) {
        DynamoDataUnit dataUnit = new DynamoDataUnit();
        dataUnit.setName(name);
        dataUnit.setTenant(testTenantId);
        dataUnit.setPartitionKey("pk");
        dataUnit.setSortKey("sk");
        dataUnit.setSignature("0000");
        return dataUnit;
    }

}
