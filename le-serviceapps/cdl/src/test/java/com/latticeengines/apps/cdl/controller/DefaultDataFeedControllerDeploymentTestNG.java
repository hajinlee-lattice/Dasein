package com.latticeengines.apps.cdl.controller;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;

public class DefaultDataFeedControllerDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final String TABLE_NAME = NamingUtils.timestamp("Table");

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironment();

        createTable(TABLE_NAME);
    }

    @Test(groups = "deployment")
    public void testUpdateNextInvokeTime() {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertNull(dataFeed.getNextInvokeTime());

        dataFeedProxy.updateDataFeedNextInvokeTime(mainCustomerSpace, new Date());

        dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertNotNull(dataFeed.getNextInvokeTime());

        dataFeedProxy.updateDataFeedNextInvokeTime(mainCustomerSpace, null);

        dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertNull(dataFeed.getNextInvokeTime());
    }

    @Test(groups = "deployment")
    public void testUpdateScheduleTime() {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertFalse(dataFeed.isScheduleNow());
        Assert.assertNull(dataFeed.getScheduleTime());

        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setUserId("userId");
        dataFeedProxy.updateDataFeedScheduleTime(mainCustomerSpace, true, request);

        dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertTrue(dataFeed.isScheduleNow());
        Assert.assertNotNull(dataFeed.getScheduleTime());
        Assert.assertNotNull(dataFeed.getScheduleRequest());

        request = JsonUtils.deserialize(dataFeed.getScheduleRequest(), ProcessAnalyzeRequest.class);
        Assert.assertNotNull(request);
        Assert.assertNotNull(request.getUserId());

        dataFeedProxy.updateDataFeedScheduleTime(mainCustomerSpace, false, null);

        dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertFalse(dataFeed.isScheduleNow());
        Assert.assertNull(dataFeed.getScheduleTime());
        Assert.assertNull(dataFeed.getScheduleRequest());
    }
}
