package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class CleanupByDateRangeDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CleanupByDateRangeDeploymentTestNG.class);
    private String customerSpace;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();

        verifyCleanupByDateRange();
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

    private List<GenericRecord> getRecords(Table table) {
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            if (!extract.getPath().endsWith("avro")) {
                paths.add(extract.getPath() + "/*.avro");
            } else {
                paths.add(extract.getPath());
            }
        }
        return AvroUtils.getDataFromGlob(yarnConfiguration, paths);
    }
}
