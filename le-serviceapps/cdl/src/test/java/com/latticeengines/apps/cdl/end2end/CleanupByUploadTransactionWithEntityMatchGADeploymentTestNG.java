package com.latticeengines.apps.cdl.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class CleanupByUploadTransactionWithEntityMatchGADeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log =
            LoggerFactory.getLogger(CleanupByUploadTransactionWithEntityMatchGADeploymentTestNG.class);

    private static final String CLEANUP_FILE_TEMPLATE = "Cleanup_Template_Transaction.csv";

    private String customerSpace;
    private Table masterTable;
    private SourceFile cleanupTemplate;
    private int originalRecordsCount;
    private int templateSize;
    private int dayPeriod;

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH_GA enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        resumeCheckpoint(ProcessTransactionWithAdvancedMatchDeploymentTestNG.CHECK_POINT);
        log.info("Setup Complete!");
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
    }

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        masterTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
        prepareCleanupTemplate();
        cleanupACPDAndVerify();
        cleanupMinDateAccountAndVerify();
        cleanupMinDateAndVerify();
    }

    private void prepareCleanupTemplate() throws IOException {
        List<GenericRecord> recordsBeforeDelete = getRecords(masterTable);
        Assert.assertTrue(recordsBeforeDelete.size() > 0);
        originalRecordsCount = recordsBeforeDelete.size();
        templateSize = 0;
        if (recordsBeforeDelete.size() > 100) {
            templateSize = 100;
        } else if (recordsBeforeDelete.size() > 10) {
            templateSize = 10;
        } else {
            templateSize = 1;
        }
        CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(CLEANUP_FILE_TEMPLATE),
                LECSVFormat.format.withHeader("AccountId", "ProductId", "TransactionTime"));
        Set<Integer> dayPeriods = new HashSet<>();
        //get records from last
        for(int i = recordsBeforeDelete.size() - 1; i >= recordsBeforeDelete.size() - templateSize; i--) {
            csvPrinter.printRecord(recordsBeforeDelete.get(i).get("CustomerAccountId").toString(),
                    recordsBeforeDelete.get(i).get("ProductId").toString(),
                    recordsBeforeDelete.get(i).get("TransactionTime").toString());
            dayPeriods.add(Integer.parseInt(recordsBeforeDelete.get(i).get("TransactionDayPeriod").toString()));
        }
        List<Integer> sortPeriods = new ArrayList<>(dayPeriods);
        Collections.sort(sortPeriods);
        dayPeriod = sortPeriods.get(0);
        csvPrinter.flush();
        csvPrinter.close();
        Resource csvResrouce = new FileSystemResource(CLEANUP_FILE_TEMPLATE);
        cleanupTemplate = uploadDeleteCSV(CLEANUP_FILE_TEMPLATE, SchemaInterpretation.DeleteTransactionTemplate,
                CleanupOperationType.BYUPLOAD_ACPD, csvResrouce);
    }

    private void cleanupACPDAndVerify() {
        ApplicationId appId = cdlProxy.cleanupByUpload(customerSpace, cleanupTemplate,
                BusinessEntity.Transaction, CleanupOperationType.BYUPLOAD_ACPD, MultiTenantContext.getEmailAddress());
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
        List<GenericRecord> records = getRecords(masterTable);
        assertTrue(records.size() + templateSize <= originalRecordsCount);
        originalRecordsCount = records.size();
    }

    private void cleanupMinDateAccountAndVerify() {
        ApplicationId appId = cdlProxy.cleanupByUpload(customerSpace, cleanupTemplate,
                BusinessEntity.Transaction, CleanupOperationType.BYUPLOAD_MINDATEANDACCOUNT, MultiTenantContext.getEmailAddress());
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
        List<GenericRecord> records = getRecords(masterTable);
        assertTrue(records.size() < originalRecordsCount);
        originalRecordsCount = records.size();
    }

    private void cleanupMinDateAndVerify() {
        ApplicationId appId = cdlProxy.cleanupByUpload(customerSpace, cleanupTemplate,
                BusinessEntity.Transaction, CleanupOperationType.BYUPLOAD_MINDATE, MultiTenantContext.getEmailAddress());
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(status, JobStatus.COMPLETED);
        List<GenericRecord> records = getRecords(masterTable);
        assertTrue(records.size() > 0);
        boolean allEarlier = true;
        for (GenericRecord record : records) {
            int period = Integer.parseInt(record.get("TransactionDayPeriod").toString());
            if (period >= dayPeriod) {
                allEarlier = false;
                break;
            }
        }
        assertTrue(allEarlier);
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
