package com.latticeengines.metadata.controller;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

    private static String DATAFEED_NAME;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        TABLE_1.setName(TABLE1);
        dataCollectionProxy.upsertTable(customerSpace1, TABLE1, TableRoleInCollection.ConsolidatedAccount);
        dataCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace1);
        datafeed = dataFeedProxy.getDataFeed(customerSpace1);
        DATAFEED_NAME = datafeed.getName();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        super.cleanup();
    }

    @Test(groups = "functional")
    public void testCrud() {
        DataFeed retrievedDataFeed = dataFeedProxy.getDataFeed(customerSpace1);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Initing);

        dataFeedProxy.updateDataFeedStatus(customerSpace1, Status.Active.getName());
        retrievedDataFeed = dataFeedProxy.getDataFeed(customerSpace1);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Active);
    }
}
