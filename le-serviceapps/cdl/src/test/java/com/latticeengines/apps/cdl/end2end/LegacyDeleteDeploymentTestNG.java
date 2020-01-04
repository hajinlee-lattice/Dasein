package com.latticeengines.apps.cdl.end2end;


import static org.testng.Assert.assertFalse;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
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
    private SourceFile cleanupTemplate;
    private int originalRecordsCount;
    private int templateSize;
    private int dayPeriod;

    @Test(groups = "end2end")
    public void testDeleteContactByUpload() throws Exception {
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        legacyDeleteByUpload();
        prepareCleanupTemplate();
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

                log.info("There are {} rows in csv.", numRecordsInCsv);
                String fileName = String.format("%s_delete_%s.csv", BusinessEntity.Contact.name(), numRecordsInCsv);
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
        log.info("There are " + recordsAfterDelete.size() + " rows in contact avro after delete.");
        Assert.assertEquals(originalNumRecords, recordsAfterDelete.size() + numRecordsInCsv);
        Table table3 = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
        List<GenericRecord> transactionRecordsAfterDelete = getRecords(table3);
        log.info("There are " + transactionRecordsAfterDelete.size() + " rows in transaction avro after delete.");
        Assert.assertTrue(transactionRecordsAfterDelete.size() + templateSize <= originalRecordsCount);
        assertFalse(HdfsUtils.fileExists(yarnConfiguration, avroDir));
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

    private void prepareCleanupTemplate() throws IOException {
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
        List<GenericRecord> recordsBeforeDelete = getRecords(table);
        Assert.assertTrue(recordsBeforeDelete.size() > 0);
        String filename = "Cleanup_Template_Transaction.csv";
        originalRecordsCount = recordsBeforeDelete.size();
        templateSize = 0;
        if (recordsBeforeDelete.size() > 100) {
            templateSize = 100;
        } else if (recordsBeforeDelete.size() > 10) {
            templateSize = 10;
        } else {
            templateSize = 1;
        }
        CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(filename),
                LECSVFormat.format.withHeader("AccountId", "ProductId", "TransactionTime"));
        Set<Integer> dayPeriods = new HashSet<>();
        //get records from last
        for(int i = recordsBeforeDelete.size() - 1; i >= recordsBeforeDelete.size() - templateSize; i--) {
            csvPrinter.printRecord(recordsBeforeDelete.get(i).get("AccountId").toString(),
                    recordsBeforeDelete.get(i).get("ProductId").toString(),
                    recordsBeforeDelete.get(i).get("TransactionTime").toString());
            dayPeriods.add(Integer.parseInt(recordsBeforeDelete.get(i).get("TransactionDayPeriod").toString()));
        }
        List<Integer> sortPeriods = new ArrayList<>(dayPeriods);
        Collections.sort(sortPeriods);
        dayPeriod = sortPeriods.get(0);
        csvPrinter.flush();
        csvPrinter.close();
        Resource csvResrouce = new FileSystemResource(filename);
        cleanupTemplate = uploadDeleteCSV(filename, SchemaInterpretation.DeleteTransactionTemplate,
                CleanupOperationType.BYUPLOAD_ACPD, csvResrouce);
        ApplicationId appId = cdlProxy.legacyDeleteByUpload(customerSpace, cleanupTemplate,
                BusinessEntity.Transaction, CleanupOperationType.BYUPLOAD_ACPD, MultiTenantContext.getEmailAddress());
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(JobStatus.COMPLETED, status);
    }

    private List<GenericRecord> getRecords(Table table) {
        Assert.assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        Assert.assertNotNull(extracts);
        List<String> paths = new ArrayList<>();
        for (Extract e : extracts) {
            paths.add(PathUtils.toAvroGlob(e.getPath()));
        }
        return AvroUtils.getDataFromGlob(yarnConfiguration, paths);
    }
}
