package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Process Account, Contact and Product for a new tenant
 */
public class ProcessAccountDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessAccountDeploymentTestNG.class);

    private static final int ACCOUNT_IMPORT_SIZE_1_1 = 350;
    private static final int ACCOUNT_IMPORT_SIZE_1_2 = 150;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        Assert.assertEquals(ACCOUNT_IMPORT_SIZE_1_1 + ACCOUNT_IMPORT_SIZE_1_2, ACCOUNT_IMPORT_SIZE_1);
        importData();
        processAnalyze();
        verifyProcess();
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockVdbImport(BusinessEntity.Account, 0, ACCOUNT_IMPORT_SIZE_1_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1_1, ACCOUNT_IMPORT_SIZE_1_2);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);
    }

}
