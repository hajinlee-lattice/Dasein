package com.latticeengines.apps.cdl.end2end.dataingestion;

import static org.junit.Assert.assertNull;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;

public class CleanupEnd2EndDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CleanupEnd2EndDeploymentTestNG.class);

    private long importedProducts;
    private long importedAccounts;
    private long importedContacts;
    private long importedTransactions;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        importData();
        processAnalyze();
        verifyCleanup(BusinessEntity.Product);
        verifyCleanup(BusinessEntity.Account);
        verifyCleanup(BusinessEntity.Contact);
        verifyCleanup(BusinessEntity.Transaction);
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        importedProducts = importCsvForCleanup(BusinessEntity.Product);
        importedAccounts = importCsvForCleanup(BusinessEntity.Account);
        importedContacts = importCsvForCleanup(BusinessEntity.Contact);
        importedTransactions = importCsvForCleanup(BusinessEntity.Transaction);

        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyCleanup(BusinessEntity entity) {
        log.info("clean up all data");
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        String tableName = dataCollectionProxy.getTableName(customerSpace, entity.getBatchStore());
        DataCollection dtCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace);

        ApplicationId appId = cdlProxy.cleanupAllData(customerSpace, entity);
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);

        log.info("assert the DataCollectionTable and MetadataTable is deleted.");
        DataCollectionTable dcTable = dataCollectionEntityMgr.getTableFromCollection(dtCollection.getName(), tableName);
        assertNull(dcTable);

        Table table = dataCollectionProxy.getTable(customerSpace, entity.getBatchStore());
        assertNull(table);

        log.info("clean up all metadata");
        appId = cdlProxy.cleanupAll(CustomerSpace.parse(mainTestTenant.getId()).toString(), entity);
        status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
        List<DataFeedTask> dfTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace, entity.name());
        assertNull(dfTasks);
    }
}
