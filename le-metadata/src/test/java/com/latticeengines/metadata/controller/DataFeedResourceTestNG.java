package com.latticeengines.metadata.controller;

import java.util.ArrayList;
import java.util.Collections;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.impl.RegisterAccountMasterMetadataTableTestNG;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class DataFeedResourceTestNG extends MetadataFunctionalTestNGBase {

    private DataCollection dc;

    private static final Table TABLE_1 = new Table();

    private DataFeed datafeed;

    @Autowired
    MetadataProxy metadataProxy;

    @Autowired
    DataCollectionProxy dataCollectionProxy;

    @Autowired
    private RegisterAccountMasterMetadataTableTestNG registerAccountMasterMetadataTableTestNG;

    private static final String DATAFEED_NAME = "datafeed";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        TABLE_1.setName(TABLE1);
        registerAccountMasterMetadataTableTestNG.registerMetadataTable();
        DataCollection dataCollection = new DataCollection();
        dataCollection.setType(DataCollectionType.Segmentation);
        dataCollection.setTables(new ArrayList<Table>(Collections.singleton(TABLE_1)));
        System.out.println("Data collection is: " + JsonUtils.serialize(dataCollection));
        dc = dataCollectionProxy.createOrUpdateDataCollection(customerSpace1, dataCollection);
        datafeed = generateDefaultDataFeed();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        super.cleanup();
    }

    DataFeed generateDefaultDataFeed() {
        DataFeed datafeed = new DataFeed();
        datafeed.setDataCollection(dc);
        datafeed.setStatus(Status.Initing);
        datafeed.setName(DATAFEED_NAME);
        datafeed.setTenant(new Tenant(customerSpace1));
        return datafeed;
    }

    @Test(groups = "functional", enabled = true)
    public void testCrud() {
        dataCollectionProxy.addDataFeed(customerSpace1, dc.getName(), datafeed);
        DataFeed retrievedDataFeed = metadataProxy.findDataFeedByName(customerSpace1, DATAFEED_NAME);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Initing);

        dataCollectionProxy.addDataFeed(customerSpace1, dc.getName(), datafeed);
        retrievedDataFeed = metadataProxy.findDataFeedByName(customerSpace1, DATAFEED_NAME);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Initing);

        metadataProxy.updateDataFeedStatus(customerSpace1, DATAFEED_NAME, Status.Active.getName());
        retrievedDataFeed = metadataProxy.findDataFeedByName(customerSpace1, DATAFEED_NAME);
        Assert.assertNotNull(retrievedDataFeed);
        Assert.assertTrue(retrievedDataFeed.getStatus() == Status.Active);
    }
}
