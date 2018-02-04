package com.latticeengines.metadata.controller;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class DataFeedResourceTestNG extends DataCollectionFunctionalTestNGBase {

    private static final Table TABLE_1 = new Table();

    private DataFeed datafeed;

    @Autowired
    MetadataProxy metadataProxy;

    @Autowired
    DataFeedProxy dataFeedProxy;

    @Autowired
    DataCollectionProxy dataCollectionProxy;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        TABLE_1.setName(TABLE1);
        DataCollection.Version version = dataCollectionEntityMgr.getActiveVersion();
        dataCollectionProxy.upsertTable(customerSpace1, TABLE1, TableRoleInCollection.ConsolidatedAccount, version);
        dataCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace1);
        datafeed = dataFeedProxy.getDataFeed(customerSpace1);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        super.cleanup();
    }

    @Test(groups = "functional")
    public void testCrud() {
        DataFeed testDefault = dataFeedProxy.getDefaultDataFeed(customerSpace1);
        Assert.assertNotNull(testDefault);
        DataFeed retrievedDataFeed = dataFeedProxy.getDataFeed(customerSpace1);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Initing);

        dataFeedProxy.updateDataFeedStatus(customerSpace1, Status.Active.getName());
        retrievedDataFeed = dataFeedProxy.getDataFeed(customerSpace1);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Active);
    }
}
