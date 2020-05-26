package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;

public class DataCollectionStatusEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataCollectionStatusEntityMgr dataCollectionStatusEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testCRUD() throws Exception {

        DataCollectionStatus status = new DataCollectionStatus();
        status.setTenant(mainTestTenant);
        status.setDataCollection(dataCollection);
        status.setVersion(DataCollection.Version.Blue);
        status.setAccountCount(10L);
        dataCollectionStatusEntityMgr.createOrUpdate(status);

        Thread.sleep(500);
        DataCollectionStatus retrievedStatus = dataCollectionStatusEntityMgr.findByTenantAndVersion(mainTestTenant,
                DataCollection.Version.Blue);
        System.out.print(JsonUtils.serialize(retrievedStatus));
        Assert.assertEquals((long) retrievedStatus.getAccountCount(), 10L);

        status.setAccountCount(20L);
        dataCollectionStatusEntityMgr.createOrUpdate(status);
        // wait 500 replication lag
        Thread.sleep(500);
        retrievedStatus = dataCollectionStatusEntityMgr.findByTenantAndVersion(mainTestTenant,
                DataCollection.Version.Blue);
        Assert.assertEquals((long) retrievedStatus.getAccountCount(), 20L);
    }

}
