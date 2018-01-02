package com.latticeengines.apps.cdl.end2end.dataingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;

public class CleanupEnd2EndDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CleanupEnd2EndDeploymentTestNG.class);

    private String avroDir;
    private Integer period = DateTimeUtils.dateToDayPeriod("2017-3-1");

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        importData();
        processAnalyze();
        verifyProfile();
        verifyCleanupByDateRange();

        verifyCleanup(BusinessEntity.Product);
        verifyCleanup(BusinessEntity.Account);
        verifyCleanup(BusinessEntity.Contact);
        verifyCleanup(BusinessEntity.Transaction);
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        importCsvForCleanup(BusinessEntity.Product);
        importCsvForCleanup(BusinessEntity.Account);
        importCsvForCleanup(BusinessEntity.Contact);
        importCsvForCleanup(BusinessEntity.Transaction);

        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProfile() throws IOException {
        try {
            DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
            assertNotNull(dataFeed);

            Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), TableRoleInCollection
                    .ConsolidatedRawTransaction);
            assertNotNull(table);
            assertNotNull(table.getExtracts());
            assertEquals(table.getExtracts().size(), 1);

            avroDir = String.format("%sPeriod-%d-data.avro", table.getExtracts().get(0).getPath(), period);
            log.info("avroDir: " + avroDir);

            assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroDir));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void verifyCleanupByDateRange() throws IOException {
        Date startTime = new Date(1488211200000l); // 2017-2-28
        Date endTime = new Date(1488384000000l); // 2017-3-2
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        ApplicationId applicationId = cdlProxy.cleanupByTimeRange(mainTestTenant.getId(),
                df.format(startTime), df.format(endTime), BusinessEntity.Transaction);

        assertNotNull(applicationId);
        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
        assertFalse(HdfsUtils.fileExists(yarnConfiguration, avroDir));
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