package com.latticeengines.apps.cdl.end2end;


import static org.testng.Assert.assertFalse;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class LegacyDeleteDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(LegacyDeleteDeploymentTestNG.class);
    private String customerSpace;

    private int numRecordsInCsv = 0;
    private int originalNumRecords;
    private String avroDir;

    @Test(groups = "end2end")
    public void testDeleteContactByUpload() throws Exception {
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        legacyDeleteByUpload();
        cleanupByDateRange();
        processAnalyze();
        verifyCleanup();
    }

    private void legacyDeleteByUpload() {
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        List<GenericRecord> recordsBeforeDelete = getRecords(table);
        originalNumRecords = recordsBeforeDelete.size();
        log.info("There are " + originalNumRecords + " rows in avro before delete.");
        String fieldName = table.getAttribute(InterfaceName.ContactId.name()).getName();
        StringBuilder sb = new StringBuilder();
        sb.append("id");
        sb.append(',');
        sb.append("index");
        sb.append('\n');
        for (GenericRecord record : recordsBeforeDelete) {
            sb.append(record.get(fieldName).toString());
            sb.append(',');
            sb.append(numRecordsInCsv);
            sb.append('\n');
            numRecordsInCsv++;
            if (numRecordsInCsv == 10 || numRecordsInCsv == 20) {

                log.info("There are " + numRecordsInCsv + " rows in csv.");
                String fileName = "contact_delete_" + numRecordsInCsv + ".csv";
                Resource source = new ByteArrayResource(sb.toString().getBytes()) {
                    @Override
                    public String getFilename() {
                        return fileName;
                    }
                };
                SourceFile sourceFile = uploadDeleteCSV(fileName, SchemaInterpretation.DeleteContactTemplate,
                        CleanupOperationType.BYUPLOAD_ID,
                        source);
                ApplicationId appId = cdlProxy.legacyDeleteByUpload(customerSpace, sourceFile,
                        BusinessEntity.Contact, CleanupOperationType.BYUPLOAD_ID, MultiTenantContext.getEmailAddress());
                JobStatus status = waitForWorkflowStatus(appId.toString(), false);
                Assert.assertEquals(JobStatus.COMPLETED, status);
                if (numRecordsInCsv == 20) {
                    break;
                }
                sb = new StringBuilder();
                sb.append("id");
                sb.append(',');
                sb.append("index");
                sb.append('\n');
            }
        }
        assert (numRecordsInCsv > 0);
    }

    private void verifyCleanup() throws IOException {
        Table table2 = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        List<GenericRecord> recordsAfterDelete = getRecords(table2);
        log.info("There are " + recordsAfterDelete.size() + " rows in avro after delete.");
        Assert.assertEquals(originalNumRecords, recordsAfterDelete.size() + numRecordsInCsv);
        assertFalse(HdfsUtils.fileExists(yarnConfiguration, avroDir));
        verifyActionRegistration();
    }

    private void cleanupByDateRange() throws ParseException {
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
        List<GenericRecord> records = getRecords(table);
        Assert.assertTrue(records.size() > 0);

        String transactionDate = records.get(0).get("TransactionDate").toString();
        String period = records.get(0).get("TransactionDayPeriod").toString();
        avroDir = String.format("%sPeriod-%s-data.avro", table.getExtracts().get(0).getPath(), period);
        log.info("avroDir: " + avroDir);
        cdlProxy.legacyDeleteByDateRange(customerSpace, transactionDate,
                transactionDate, BusinessEntity.Transaction, MultiTenantContext.getEmailAddress());

    }

    private List<GenericRecord> getRecords(Table table) {
        Assert.assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        Assert.assertNotNull(extracts);
        List<String> paths = new ArrayList<>();
        for (Extract e : extracts) {
            paths.add(e.getPath());
        }
        return AvroUtils.getDataFromGlob(yarnConfiguration, paths);
    }
}
