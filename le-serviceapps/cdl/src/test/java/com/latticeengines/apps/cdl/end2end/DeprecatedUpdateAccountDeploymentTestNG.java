package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.apps.cdl.end2end.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.ACCOUNT_IMPORT_SIZE_OVERLAP;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.ACCOUNT_IMPORT_SIZE_TOTAL;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.TRANSACTION_IMPORT_SIZE_1;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class DeprecatedUpdateAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DeprecatedUpdateAccountDeploymentTestNG.class);

    static final String CHECK_POINT = "update1";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);

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
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1 - ACCOUNT_IMPORT_SIZE_OVERLAP,
                ACCOUNT_IMPORT_SIZE_2);
        Thread.sleep(2000);
    }

    private void verifyProcess() {
        runCommonPAVerifications();

        verifyProcessAnalyzeReport(processAnalyzeAppId);

        long numAccounts = ACCOUNT_IMPORT_SIZE_TOTAL;
        long numContacts = CONTACT_IMPORT_SIZE_1;
        long numProducts = PRODUCT_IMPORT_SIZE_1;
        long numTransactions = TRANSACTION_IMPORT_SIZE_1;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_2,
                BusinessEntity.Contact, SEGMENT_1_CONTACT_2);
        verifyTestSegment1Counts(segment1Counts);
    }

}