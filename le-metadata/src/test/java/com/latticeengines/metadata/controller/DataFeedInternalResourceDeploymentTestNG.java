package com.latticeengines.metadata.controller;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class DataFeedInternalResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DataFeedInternalResourceDeploymentTestNG.class);

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();
        dataFeedProxy.getDataFeed(customerSpace1);
    }

    @AfterTest(groups = { "deployment" })
    public void teardown() throws Exception {
        super.cleanup();
    }

    @Test(groups = "deployment")
    public void testGetAllDataFeeds() throws IOException {
        List<DataFeed> dataFeeds = dataFeedProxy.getAllDataFeeds();
        assertNotNull(dataFeeds);
        assertTrue(dataFeeds.size() > 0);

        boolean hasTheCreateOne = false;
        for(DataFeed dataFeed : dataFeeds) {
            if( dataFeed.getTenantId() == tenant1.getPid()) {
                hasTheCreateOne = true;
            }
        }

        assertTrue(hasTheCreateOne);
    }

    @Test(groups = "deployment")
    public void testGetAllSimpleDataFeeds() throws IOException {
        List<SimpleDataFeed> simpleDataFeeds = dataFeedProxy.getAllSimpleDataFeeds();
        assertNotNull(simpleDataFeeds);
        assertTrue(simpleDataFeeds.size() > 0);

        boolean hasTheCreateOne = false;
        for(SimpleDataFeed simpleDataFeed : simpleDataFeeds) {
            if(simpleDataFeed.getTenant().getId().equals(customerSpace1)) {
                hasTheCreateOne = true;
            }
        }

        assertTrue(hasTheCreateOne);
    }
}
