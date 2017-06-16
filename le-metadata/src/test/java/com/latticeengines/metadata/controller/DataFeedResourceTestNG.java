package com.latticeengines.metadata.controller;

import java.util.Collections;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class DataFeedResourceTestNG extends DataCollectionFunctionalTestNGBase {

    private static final Table TABLE_1 = new Table();

    private DataFeed datafeed;

    @Autowired
    MetadataProxy metadataProxy;

    @Autowired
    DataCollectionProxy dataCollectionProxy;

    private static final String DATAFEED_NAME = "datafeed";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        TABLE_1.setName(TABLE1);
        dataCollection = new DataCollection();
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollection.setTables(Collections.singletonList(TABLE_1));
        dataCollection = dataCollectionProxy.createOrUpdateDataCollection(customerSpace1, dataCollection);
        dataCollectionProxy.upsertTable(customerSpace1, dataCollection.getName(), TABLE1, TableRoleInCollection.ConsolidatedAccount);
        dataCollection = dataCollectionProxy.getDataCollection(customerSpace1, dataCollection.getName());
        datafeed = generateDefaultDataFeed();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        super.cleanup();
    }

    DataFeed generateDefaultDataFeed() {
        DataFeed datafeed = new DataFeed();
        datafeed.setDataCollection(dataCollection);
        datafeed.setStatus(Status.Initing);
        datafeed.setName(DATAFEED_NAME);
        datafeed.setTenant(new Tenant(customerSpace1));
        return datafeed;
    }

    @Test(groups = "functional")
    public void testCrud() {
        dataCollectionProxy.addDataFeed(customerSpace1, dataCollection.getName(), datafeed);
        DataFeed retrievedDataFeed = metadataProxy.findDataFeedByName(customerSpace1, DATAFEED_NAME);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Initing);

        dataCollectionProxy.addDataFeed(customerSpace1, dataCollection.getName(), datafeed);
        retrievedDataFeed = metadataProxy.findDataFeedByName(customerSpace1, DATAFEED_NAME);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Initing);

        metadataProxy.updateDataFeedStatus(customerSpace1, DATAFEED_NAME, Status.Active.getName());
        retrievedDataFeed = metadataProxy.findDataFeedByName(customerSpace1, DATAFEED_NAME);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Active);
    }
}
