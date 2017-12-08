package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Process Transaction imports after ProcessAccountDeploymentTestNG
 */
public class ProcessTransactionDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDeploymentTestNG.class);

    private static final int TRANSACTION_IMPORT_SIZE_1_1 = 20000;
    private static final int TRANSACTION_IMPORT_SIZE_1_2 = 10000;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        Assert.assertEquals(TRANSACTION_IMPORT_SIZE_1_1 + TRANSACTION_IMPORT_SIZE_1_2, TRANSACTION_IMPORT_SIZE_1);
        resumeCheckpoint("process1");
        importData();
        processAnalyze();
        verifyProcess();
        // saveCheckpoint("process2");
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockVdbImport(BusinessEntity.Transaction, 0, TRANSACTION_IMPORT_SIZE_1_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Transaction, TRANSACTION_IMPORT_SIZE_1_1, TRANSACTION_IMPORT_SIZE_1_2);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        // verifyActiveVersion(DataCollection.Version.Green);
        long numTransactions = countTableRole(TableRoleInCollection.ConsolidatedRawTransaction);
        Assert.assertEquals(numTransactions, TRANSACTION_IMPORT_SIZE_1);
    }

}
