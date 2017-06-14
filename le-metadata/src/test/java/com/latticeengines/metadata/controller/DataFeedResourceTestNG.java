package com.latticeengines.metadata.controller;

import java.util.ArrayList;
import java.util.Collections;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class DataFeedResourceTestNG extends MetadataFunctionalTestNGBase {

    private DataCollection dataCollection;

    private DataFeed datafeed;

    @Autowired
    MetadataProxy metadataProxy;

    private static final String DATAFEED_NAME = "datafeed";

    @SuppressWarnings("unchecked")
    @BeforeClass(groups = "functional")
    public void setup() {
        dataCollection = new DataCollection();
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollection.setTables(new ArrayList<Table>(Collections.singleton(new Table())));
        datafeed = generateDefaultDataFeed();
        super.setup();
    }

    DataFeed generateDefaultDataFeed() {
        DataFeed datafeed = new DataFeed();
        datafeed.setDataCollection(dataCollection);
        datafeed.setStatus(Status.Initing);
        datafeed.setName(DATAFEED_NAME);
        datafeed.setTenant(new Tenant(customerSpace1));
        datafeed.setActiveExecutionId(1L);
        return datafeed;
    }

    @Test(groups = "functional", enabled = false)
    public void testCrud() {
        DataFeed df = metadataProxy.createDataFeed(customerSpace1, datafeed);
        DataFeed retrievedDataFeed = metadataProxy.findDataFeedByName(customerSpace1.toString(), DATAFEED_NAME);
        Assert.assertNotNull(retrievedDataFeed);
        metadataProxy.updateDataFeedStatus(customerSpace1.toString(), DATAFEED_NAME, Status.Active);
        retrievedDataFeed = metadataProxy.findDataFeedByName(customerSpace1.toString(), DATAFEED_NAME);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Active);
    }
}
