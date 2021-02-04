package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.List;

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

    private static final String DATATEMPLATE_ID = "DataTempLateId";

    @BeforeClass(groups = "functional")
    public void setup() {
        functionalTestBed.bootstrap(1);
        Tenant testTenant = functionalTestBed.getMainTestTenant();
        MultiTenantContext.setTenant(testTenant);
        String customerSpace = CustomerSpace.parse(testTenant.getId()).toString();
        testTenantId = CustomerSpace.shortenCustomerSpace(customerSpace);

    }

    @Test(groups = "functional")
    public void testFindAllDataUnitEntitiesWithExpiredRetentionPolicy() {
        String name = NamingUtils.timestampWithRandom("Dynamo");
        DataUnit unit = dataUnitService.createOrUpdateByNameAndStorageType(createDynamoUnit(name));

        name = NamingUtils.timestampWithRandom("Dynamo");
        unit = dataUnitService.createOrUpdateByNameAndStorageType(createDynamoUnit(name));

        name = NamingUtils.timestampWithRandom("Dynamo");
        unit = dataUnitService.createOrUpdateByNameAndStorageType(createDynamoUnit(name));

        name = NamingUtils.timestampWithRandom("Dynamo");
        unit = dataUnitService.createOrUpdateByNameAndStorageType(createDynamoUnit(name));

        name = NamingUtils.timestampWithRandom("Dynamo");
        unit = dataUnitService.createOrUpdateByNameAndStorageType(createDynamoUnit(name));


        Assert.assertNotNull(unit);

    }

    private DynamoDataUnit createDynamoUnit(String name) {
        DynamoDataUnit dataUnit = new DynamoDataUnit();
        dataUnit.setName(name);
        dataUnit.setTenant(testTenantId);
        dataUnit.setPartitionKey("pk");
        dataUnit.setSortKey("sk");
        dataUnit.setSignature("0000");
        dataUnit.setDataTemplateId(DATATEMPLATE_ID);
        List<DataUnit.Role> roles = new ArrayList<DataUnit.Role>();
        roles.add(DataUnit.Role.Master);
        roles.add(DataUnit.Role.Snapshot);
        dataUnit.setRoles(roles);
        return dataUnit;
    }

}
