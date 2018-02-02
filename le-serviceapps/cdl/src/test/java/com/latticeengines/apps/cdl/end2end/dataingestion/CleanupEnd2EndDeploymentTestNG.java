package com.latticeengines.apps.cdl.end2end.dataingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class CleanupEnd2EndDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CleanupEnd2EndDeploymentTestNG.class);
    private String customerSpace;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeVdbCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();

        verifyCleanupByDateRange();

//        verifyCleanup(BusinessEntity.Product);
//        verifyCleanup(BusinessEntity.Account);
//        verifyCleanup(BusinessEntity.Contact);
//        verifyCleanup(BusinessEntity.Transaction);
    }

    private void verifyCleanupByDateRange() throws IOException, ParseException {
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
        List<GenericRecord> records = getRecords(table);
        Assert.assertTrue(records.size() > 0);

        String transactionDate = records.get(0).get("TransactionDate").toString();
        String period = records.get(0).get("TransactionDayPeriod").toString();
        String avroDir = String.format("%sPeriod-%s-data.avro", table.getExtracts().get(0).getPath(), period);
        log.info("avroDir: " + avroDir);

        ApplicationId applicationId = cdlProxy.cleanupByTimeRange(customerSpace, transactionDate,
                transactionDate, BusinessEntity.Transaction, MultiTenantContext.getEmailAddress());

        assertNotNull(applicationId);
        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
        assertFalse(HdfsUtils.fileExists(yarnConfiguration, avroDir));
        verifyActionRegistration();
    }

    private void verifyCleanup(BusinessEntity entity) {
        log.info(String.format("clean up all data for entity %s, current action number is %d", entity.toString(),
                actionsNumber));

        String tableName = dataCollectionProxy.getTableName(customerSpace, entity.getBatchStore());
        DataCollection dtCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace);

        ApplicationId appId = cdlProxy.cleanupAllData(customerSpace, entity, MultiTenantContext.getEmailAddress());
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);

        log.info("assert the DataCollectionTable and MetadataTable is deleted.");
        DataCollectionTable dcTable = dataCollectionEntityMgr.getTableFromCollection(dtCollection.getName(), tableName);
        assertNull(dcTable);

        Table table = dataCollectionProxy.getTable(customerSpace, entity.getBatchStore());
        assertNull(table);

        log.info("clean up all metadata");
        appId = cdlProxy.cleanupAll(CustomerSpace.parse(mainTestTenant.getId()).toString(), entity,
                MultiTenantContext.getEmailAddress());
        status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
        List<DataFeedTask> dfTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace, entity.name());
        assertNull(dfTasks);
        verifyActionRegistration();
    }

    private List<GenericRecord> getRecords(Table table) {
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            if (!extract.getPath().endsWith("avro")) {
                paths.add(extract.getPath() + "/*.avro");
            } else {
                paths.add(extract.getPath());
            }
        }
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, paths);
        return records;
    }
}