package com.latticeengines.metadata.controller;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class DefaultDataFeedControllerDeploymentTestNG extends MetadataDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DefaultDataFeedControllerDeploymentTestNG.class);

    @Autowired
    private DataFeedProxy dataFeedProxy;

    private DataFeed defaultDataFeed;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();
        defaultDataFeed = dataFeedProxy.getDataFeed(customerSpace1);
    }

    @AfterTest(groups = { "deployment" })
    public void teardown() throws Exception {
        super.cleanup();
    }

    @Test(groups = "deployment")
    public void testGetDefaultDataFeed() throws IOException {
        DataFeed dataFeed = dataFeedProxy.getDefaultDataFeed(customerSpace1);
        assertNotNull(dataFeed);

        assertEquals(dataFeed.getPid(), defaultDataFeed.getPid());
    }
}
