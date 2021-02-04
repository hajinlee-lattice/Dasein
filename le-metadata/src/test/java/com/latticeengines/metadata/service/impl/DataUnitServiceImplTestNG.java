package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
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
    public void testCrud() throws Exception {
        String name = NamingUtils.timestamp("Dynamo");
        DataUnit unit = dataUnitService.createOrUpdateByNameAndStorageType(createDynamoUnit(name));
        Assert.assertNotNull(unit);
        Assert.assertTrue(unit instanceof DynamoDataUnit);

        Thread.sleep(500);
        List<DataUnit> found = dataUnitService.findByNameFromReader(name);
        Assert.assertTrue(CollectionUtils.isNotEmpty(found));
        Assert.assertTrue(found.get(0) instanceof DynamoDataUnit);

        String signature = "0001";
        dataUnitService.updateSignature(unit, signature);
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            DynamoDataUnit dynamoDataUnit = (DynamoDataUnit) dataUnitService.findByNameTypeFromReader(name, DataUnit.StorageType.Dynamo);
            Assert.assertEquals(dynamoDataUnit.getSignature(), signature);
            return true;
        });

        dataUnitService.deleteByNameAndStorageType(name, DataUnit.StorageType.Dynamo);
        retry.execute(context -> {
            Assert.assertNull(dataUnitService.findByNameTypeFromReader(name, DataUnit.StorageType.Dynamo));
            return true;
        });
    }

    @Test(groups = "functional")
    public void testFindByDataTemplateIdAndRole() {
        String name = NamingUtils.timestamp("Dynamo");
        DataUnit unit = dataUnitService.createOrUpdateByNameAndStorageType(createDynamoUnit(name));
        Assert.assertNotNull(unit);
        Assert.assertTrue(unit instanceof DynamoDataUnit);

        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            List<DataUnit> found = dataUnitService.findAllByDataTemplateIdAndRole(DATATEMPLATE_ID, DataUnit.Role.Master);
            Assert.assertTrue(CollectionUtils.isNotEmpty(found));
            Assert.assertTrue(found.get(0) instanceof DynamoDataUnit);
            return true;
        });

        dataUnitService.deleteByNameAndStorageType(name, DataUnit.StorageType.Dynamo);
        retry.execute(context -> {
            Assert.assertNull(dataUnitService.findByNameTypeFromReader(name, DataUnit.StorageType.Dynamo));
            return true;
        });
    }

    @Test(groups = "functional")
    public void testFindAllDataUnitEntitiesWithExpiredRetentionPolicy() {
        String name = NamingUtils.timestamp("Dynamo");
        DataUnit unit = dataUnitService.createOrUpdateByNameAndStorageType(createDynamoUnit(name));
        Assert.assertNotNull(unit);
        Assert.assertTrue(unit instanceof DynamoDataUnit);

        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            List<DataUnit> found = dataUnitService.findAllDataUnitEntitiesWithExpiredRetentionPolicy(0, 5);
            Assert.assertTrue(CollectionUtils.isNotEmpty(found));
            Assert.assertNotNull(found.get(0).getRetentionPolicy());
            Assert.assertNotEquals(found.get(0).getRetentionPolicy(), RetentionPolicyUtil.NEVER_EXPIRE_POLICY);
            return true;
        });

        dataUnitService.deleteByNameAndStorageType(name, DataUnit.StorageType.Dynamo);
        retry.execute(context -> {
            Assert.assertNull(dataUnitService.findByNameTypeFromReader(name, DataUnit.StorageType.Dynamo));
            return true;
        });
    }

    private DynamoDataUnit createDynamoUnit(String name) {
        DynamoDataUnit dataUnit = new DynamoDataUnit();
        dataUnit.setName(name);
        dataUnit.setTenant(testTenantId);
        dataUnit.setPartitionKey("pk");
        dataUnit.setSortKey("sk");
        dataUnit.setSignature("0000");
        dataUnit.setDataTemplateId(DATATEMPLATE_ID);
        dataUnit.setRetentionPolicy("KEEP_3_DAYS");
        List<DataUnit.Role> roles = new ArrayList<DataUnit.Role>();
        roles.add(DataUnit.Role.Master);
        roles.add(DataUnit.Role.Import);
        dataUnit.setRoles(roles);
        return dataUnit;
    }

}
