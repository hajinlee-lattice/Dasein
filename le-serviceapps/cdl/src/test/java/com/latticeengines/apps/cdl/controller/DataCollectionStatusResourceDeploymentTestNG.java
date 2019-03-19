package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

public class DataCollectionStatusResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void testCrud() throws Exception {
        // post empty node, verify insert default node
        DataCollectionStatus emptyStatus = new DataCollectionStatus();
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, emptyStatus,
                DataCollection.Version.Blue);
        Thread.sleep(500);
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(mainCustomerSpace,
                DataCollection.Version.Blue);
        // verify default value
        Assert.assertTrue(status.getAccountCount() == 0L);
        Assert.assertTrue(status.getMaxTxnDate() == 0);

        // insert status history
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, emptyStatus);
        Thread.sleep(1000);
        List<DataCollectionStatusHistory> statusHisList = dataCollectionProxy
                .getDataCollectionStatusHistory(mainCustomerSpace);
        Assert.assertEquals(statusHisList.size(), 1);
        DataCollectionStatusHistory statusHis = statusHisList.get(0);
        Assert.assertTrue(statusHis.getAccountCount() == 0L);
        Assert.assertTrue(statusHis.getContactCount() == 0L);

        // insert blue
        status.setAccountCount(10L);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, status,
                DataCollection.Version.Blue);
        Thread.sleep(500);
        DataCollectionStatus blueStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(
                mainCustomerSpace,
                DataCollection.Version.Blue);
        Assert.assertTrue(blueStatus.getAccountCount() == 10L);

        // insert status history with version blue, verify first item in array
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, status);
        Thread.sleep(1000);
        statusHisList = dataCollectionProxy.getDataCollectionStatusHistory(mainCustomerSpace);
        Assert.assertEquals(statusHisList.size(), 2);
        statusHis = statusHisList.get(0);
        Assert.assertTrue(statusHis.getAccountCount() == 10L);

        // insert green
        status.setAccountCount(20L);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, status,
                DataCollection.Version.Green);
        Thread.sleep(500);
        DataCollectionStatus greenStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(mainCustomerSpace,
                DataCollection.Version.Green);
        Assert.assertTrue(greenStatus.getAccountCount() == 20L);

        // insert status history with version green, verify first item
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, status);
        Thread.sleep(1000);
        statusHisList = dataCollectionProxy.getDataCollectionStatusHistory(mainCustomerSpace);
        Assert.assertEquals(statusHisList.size(), 3);
        statusHis = statusHisList.get(0);
        Assert.assertTrue(statusHis.getAccountCount() == 20L);
        // verify status history with version blue, no change
        statusHis = statusHisList.get(1);
        Assert.assertTrue(statusHis.getAccountCount() == 10L);

        // update blue, verify green
        blueStatus.setAccountCount(30L);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, blueStatus,
                DataCollection.Version.Blue);
        greenStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(mainCustomerSpace,
                DataCollection.Version.Green);
        Assert.assertTrue(greenStatus.getAccountCount() == 20L);

        // insert history
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, blueStatus);

        // update green, verify blue
        greenStatus.setAccountCount(40L);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, greenStatus,
                DataCollection.Version.Green);
        blueStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(mainCustomerSpace,
                DataCollection.Version.Blue);
        Assert.assertTrue(blueStatus.getAccountCount() == 30L);

        Thread.sleep(1000);
        // insert history
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, greenStatus);

        statusHisList = dataCollectionProxy.getDataCollectionStatusHistory(mainCustomerSpace);
        System.out.println("info : " + JsonUtils.serialize(statusHisList));
        Assert.assertEquals(statusHisList.size(), 5);
        statusHis = statusHisList.get(0);
        Assert.assertTrue(statusHis.getAccountCount() == 40L);
        statusHis = statusHisList.get(1);
        Assert.assertTrue(statusHis.getAccountCount() == 30L);
    }
}
