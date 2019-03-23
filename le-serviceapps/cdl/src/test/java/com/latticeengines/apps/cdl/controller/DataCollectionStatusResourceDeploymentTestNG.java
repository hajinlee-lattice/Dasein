package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
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
        DataCollectionStatus status = new DataCollectionStatus();
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, status,
                DataCollection.Version.Blue);
        verfyStatusWithRetries(DataCollection.Version.Blue, 0L);

        // insert status history
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, status);
        // make sure the following two entries have different creation time,
        // time value in DB are accurate to second
        Thread.sleep(1000);
        verifyStatusHistoryWithRetries(1, 0L, false, 0L);

        // insert blue
        status.setAccountCount(10L);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, status,
                DataCollection.Version.Blue);
        verfyStatusWithRetries(DataCollection.Version.Blue, 10L);

        // insert status history with version blue, verify first item in array
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, status);
        Thread.sleep(1000);
        verifyStatusHistoryWithRetries(2, 10L, false, 0L);

        // insert green
        status.setAccountCount(20L);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, status,
                DataCollection.Version.Green);
        verfyStatusWithRetries(DataCollection.Version.Green, 20L);

        // insert status history with version green, verify first item
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, status);
        Thread.sleep(1000);
        verifyStatusHistoryWithRetries(3, 20L, true, 10L);

        // update blue, verify green
        status.setAccountCount(30L);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, status,
                DataCollection.Version.Blue);
        verfyStatusWithRetries(DataCollection.Version.Green, 20L);


        // insert history
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, status);

        // update green, verify blue
        status.setAccountCount(40L);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainCustomerSpace, status,
                DataCollection.Version.Green);
        verfyStatusWithRetries(DataCollection.Version.Blue, 30L);

        Thread.sleep(1000);
        // insert history
        dataCollectionProxy.saveDataCollectionStatusHistory(mainCustomerSpace, status);

        verifyStatusHistoryWithRetries(5, 40L, true, 30L);
    }

    private void verifyStatusHistoryWithRetries(int size, long accountNumber1, boolean verifySecond,
            long accountNumber2) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(2, Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            List<DataCollectionStatusHistory> statusHisList = dataCollectionProxy
                    .getDataCollectionStatusHistory(mainCustomerSpace);
            Assert.assertEquals(statusHisList.size(), size);
            DataCollectionStatusHistory statusHis = statusHisList.get(0);
            Assert.assertTrue(statusHis.getAccountCount() == accountNumber1);
            Assert.assertTrue(statusHis.getContactCount() == 0L);
            if (verifySecond) {
                statusHis = statusHisList.get(1);
                Assert.assertTrue(statusHis.getAccountCount() == accountNumber2);
            }
            return true;
        });
    }

    private void verfyStatusWithRetries(DataCollection.Version version, long accountNumber) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(2,
                Arrays.asList(AssertionError.class, NullPointerException.class), null);
        retry.execute(context -> {
            DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(mainCustomerSpace,
                    version);
            Assert.assertTrue(status.getAccountCount() == accountNumber);
            Assert.assertTrue(status.getMaxTxnDate() == 0);
            return true;
        });
    }
}
