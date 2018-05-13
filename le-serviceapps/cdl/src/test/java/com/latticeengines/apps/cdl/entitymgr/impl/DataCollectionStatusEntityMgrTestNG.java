package com.latticeengines.apps.cdl.entitymgr.impl;
import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;

public class DataCollectionStatusEntityMgrTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DataCollectionStatusEntityMgr dataCollectionStatusEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testCRUD() throws Exception {

        DataCollectionStatus status = new DataCollectionStatus();
        DataCollectionStatusDetail detail = new DataCollectionStatusDetail();
        detail.setAccountCount(0L);
        status.setTenant(mainTestTenant);
        status.setDataCollection(dataCollection);
        status.setVersion(DataCollection.Version.Blue);
        status.setDetail(detail);
        dataCollectionStatusEntityMgr.saveStatus(status);
        // wait 500 replication lag
        Thread.sleep(500);

        DataCollectionStatus retrievedStatus = dataCollectionStatusEntityMgr.findByTenant(mainTestTenant);
        DataCollectionStatusDetail retrievedDetail = retrievedStatus.getDetail();
        Assert.assertTrue(retrievedDetail.getAccountCount() == 0L);

        detail.setAccountCount(10L);
        status.setDetail(detail);
        dataCollectionStatusEntityMgr.saveStatus(status);
        // wait 500 replication lag
        Thread.sleep(500);
        retrievedStatus = dataCollectionStatusEntityMgr.findByTenant(mainTestTenant);
        retrievedDetail = retrievedStatus.getDetail();
        Assert.assertTrue(retrievedDetail.getAccountCount() == 10L);
    }

}
