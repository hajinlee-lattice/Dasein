package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_OVERLAP;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_TOTAL;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_1;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class UpdateAccountDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountDeploymentTestNG.class);

    static final String CHECK_POINT = "update1";

    private static final long DEPIVOTED_METRICS_SIZE = 7871L;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);

        // For the test scenario of non-1st profile purchase history
        dataFeedProxy.updateEarliestLatestTransaction(mainTestTenant.getId(), EARLIEST_TRANSACTION, LATEST_TRANSACTION);

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