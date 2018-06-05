package com.latticeengines.apps.cdl.end2end;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class UpdateAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountDeploymentTestNG.class);

    static final String CHECK_POINT = "update1";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), 500);

        new Thread(this::createTestSegment1).start();

        importData();
        processAnalyze();

        try {
            verifyProcess();
        } finally {
            saveCheckpoint(CHECK_POINT);
        }

    }

    private void importData() throws Exception {
        mockCSVImport(BusinessEntity.Account, 3, "Account");
        Thread.sleep(2000);
    }

    private void verifyProcess() {
        runCommonPAVerifications();
        verifyProcessAnalyzeReport(processAnalyzeAppId);

//        long numAccounts = 1000;
//        long numContacts = 500;
//        long numProducts = 99;
//        long numTransactions = ???;
//
//        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
//        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);
//
//        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
//        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);
//
//        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
//                BusinessEntity.Account, SEGMENT_1_ACCOUNT_2,
//                BusinessEntity.Contact, SEGMENT_1_CONTACT_2);
//        verifyTestSegment1Counts(segment1Counts);
    }

}