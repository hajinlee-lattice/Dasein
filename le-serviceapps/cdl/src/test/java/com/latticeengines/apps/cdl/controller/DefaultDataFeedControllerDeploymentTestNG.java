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
import com.latticeengines.common.exposed.util.NamingUtils;
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
    public void test() {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertNull(dataFeed.getNextInvokeTime());

        dataFeedProxy.updateDataFeedNextInvokeTime(mainCustomerSpace, new Date());

        dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertNotNull(dataFeed.getNextInvokeTime());

        dataFeedProxy.updateDataFeedNextInvokeTime(mainCustomerSpace, null);

        dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertNull(dataFeed.getNextInvokeTime());
    }
}
