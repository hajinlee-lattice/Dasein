package com.latticeengines.apps.cdl.controller;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.impl.CheckpointService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class OrphanRecordExportDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(OrphanRecordExportDeploymentTestNG.class);
    private static final String CHECK_POINT = "orphan";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss";
    private static final String NUM_RECORDS = "NUM_RECORDS";
    private static final String MERGED_FILENAME_PREFIX = "MERGED_FILENAME_PREFIX";
    private static final String TARGET_FILE_SUFFIX = "TARGET_FILE_SUFFIX";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private CheckpointService checkpointService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        // setup test tenant
        super.setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        log.info("TenantId=" + MultiTenantContext.getTenant().getId());
    }

    @AfterClass(groups = "deployment")
    protected void cleanup() {
        checkpointService.cleanup();
    }

    @Test(groups = "deployment")
    public void testInvalidOrphanReportExport() throws Exception {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();

        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.TRANSACTION);
        ApplicationId appid = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        Assert.assertNull(appid);

        request = createExportJob(OrphanRecordsType.CONTACT);
        appid = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        Assert.assertNull(appid);

        request = createExportJob(OrphanRecordsType.UNMATCHED_ACCOUNT);
        appid = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        Assert.assertNull(appid);

        // setup checkpoint
        checkpointService.resumeCheckpoint(CHECK_POINT, 19);
    }

    @Test(groups = "deployment", priority = 1, dependsOnMethods = "testInvalidOrphanReportExport")
    public void testOrphanTransactionExport() {
        log.info("Running testOrphanTransactionExport() ...");
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.TRANSACTION);
        log.info("OrphanRecordsExportRequest=" + JsonUtils.serialize(request));

        ApplicationId appid = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId=" + appid.toString());

        JobStatus status = waitForWorkflowStatus(appid.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);

        Job job = workflowProxy.getWorkflowJobFromApplicationId(appid.toString(), customerSpace);
        String targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);

        Map<String, Object> expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 9308);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.TRANSACTION.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);
    }

    @Test(groups = "deployment", priority = 2, dependsOnMethods = "testInvalidOrphanReportExport")
    public void testOrphanContactExport() {
        log.info("Running testOrphanContactExport() ...");
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.CONTACT);
        log.info("OrphanRecordsExportRequest=" + JsonUtils.serialize(request));

        ApplicationId appid = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId=" + appid.toString());

        JobStatus status = waitForWorkflowStatus(appid.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);

        Job job = workflowProxy.getWorkflowJobFromApplicationId(appid.toString(), customerSpace);
        String targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);

        Map<String, Object> expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 3);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.CONTACT.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);
    }

    @Test(groups = "deployment", priority = 3, dependsOnMethods = "testInvalidOrphanReportExport")
    public void testUnmatchedAccountExport() {
        log.info("Running testUnmatchedAccountExport() ...");
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.UNMATCHED_ACCOUNT);
        log.info("OrphanRecordsExportRequest=" + JsonUtils.serialize(request));

        ApplicationId appid = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId=" + appid.toString());

        JobStatus status = waitForWorkflowStatus(appid.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);

        Job job = workflowProxy.getWorkflowJobFromApplicationId(appid.toString(), customerSpace);
        String targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);

        Map<String, Object> expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 9);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.UNMATCHED_ACCOUNT.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);
    }

    @Test(groups = "deployment", priority = 4, dependsOnMethods = "testInvalidOrphanReportExport")
    public void testMultipleOrphanTypesExport() {
        log.info("Running testMultipleOrphanTypesExport() ...");
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();

        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.UNMATCHED_ACCOUNT);
        ApplicationId accountAppId = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId for unmatched accounts: " + accountAppId.toString());

        request = createExportJob(OrphanRecordsType.TRANSACTION);
        ApplicationId transactionAppId = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId for orphan transactions: " + transactionAppId.toString());

        request = createExportJob(OrphanRecordsType.CONTACT);
        ApplicationId contactAppId = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId for orphan contacts: " + contactAppId.toString());

        JobStatus status = waitForWorkflowStatus(accountAppId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        Job job = workflowProxy.getWorkflowJobFromApplicationId(accountAppId.toString(), customerSpace);
        String targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);
        Map<String, Object> expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 9);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.UNMATCHED_ACCOUNT.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);

        status = waitForWorkflowStatus(transactionAppId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        job = workflowProxy.getWorkflowJobFromApplicationId(transactionAppId.toString(), customerSpace);
        targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);
        expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 9308);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.TRANSACTION.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);

        status = waitForWorkflowStatus(contactAppId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        job = workflowProxy.getWorkflowJobFromApplicationId(contactAppId.toString(), customerSpace);
        targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);
        expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 3);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.CONTACT.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);
    }

    @Test(groups = "deployment", priority = 5, dependsOnMethods = "testInvalidOrphanReportExport")
    public void testSameOrphanTypesExport() {
        log.info("Running testSameOrphanTypesExport() ...");
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.TRANSACTION);
        ApplicationId applicationId1 = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId for orphan transactions 1: " + applicationId1.toString());

        request = createExportJob(OrphanRecordsType.TRANSACTION);
        ApplicationId applicationId2 = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId for orphan transactions 2: " + applicationId2.toString());

        request = createExportJob(OrphanRecordsType.TRANSACTION);
        ApplicationId applicationId3 = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId for orphan transactions 3: " + applicationId3.toString());

        JobStatus status = waitForWorkflowStatus(applicationId1.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId1.toString(), customerSpace);
        String targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);
        Map<String, Object> expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 9308);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.TRANSACTION.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);

        status = waitForWorkflowStatus(applicationId2.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        job = workflowProxy.getWorkflowJobFromApplicationId(applicationId2.toString(), customerSpace);
        targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);
        expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 9308);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.TRANSACTION.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);

        status = waitForWorkflowStatus(applicationId3.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        job = workflowProxy.getWorkflowJobFromApplicationId(applicationId3.toString(), customerSpace);
        targetPathSuffix = job.getInputs().get(ExportProperty.TARGET_FILE_NAME);
        expectedResults = new HashMap<>();
        expectedResults.put(NUM_RECORDS, 9308);
        expectedResults.put(MERGED_FILENAME_PREFIX, OrphanRecordsType.TRANSACTION.getOrphanType());
        expectedResults.put(TARGET_FILE_SUFFIX, targetPathSuffix);
        verifyResults(expectedResults);
    }

    private void verifyResults(Map<String, Object> expectedResults) {
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        String dir = String.format("/Pods/Default/Contracts/%s/Tenants/%s/Spaces/Production/Data/Files/Exports/%s",
                tenantId, tenantId, expectedResults.get(TARGET_FILE_SUFFIX));
        log.info("Looking for file in path " + dir);
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dir,
                    expectedResults.get(MERGED_FILENAME_PREFIX) + "_.*.csv$");
            Assert.assertNotNull(files);
            int totalRecordNum = 0;
            for (String file : files) {
                CSVParser parser = new CSVParser(
                        new InputStreamReader(HdfsUtils.getInputStream(yarnConfiguration, file)), LECSVFormat.format);
                List<CSVRecord> records = parser.getRecords();
                totalRecordNum += records.size();
            }
            Assert.assertEquals(totalRecordNum, ((Integer) expectedResults.get(NUM_RECORDS)).intValue());
        } catch (IOException e) {
            log.error("Failed to parse csv file", e);
        }
    }

    private OrphanRecordsExportRequest createExportJob(OrphanRecordsType type) {
        OrphanRecordsExportRequest request = new OrphanRecordsExportRequest();
        request.setOrphanRecordsType(type);
        request.setOrphanRecordsArtifactStatus(DataCollectionArtifact.Status.NOT_SET);
        request.setCreatedBy(CREATED_BY);
        request.setExportId(UUID.randomUUID().toString());
        request.setArtifactVersion(null);

        return request;
    }
}
