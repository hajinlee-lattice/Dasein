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
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class OrphanRecordExportDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(OrphanRecordExportDeploymentTestNG.class);
    private static final String CHECK_POINT = "orphan";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss";
    private static final String NUM_FILES = "NUM_FILES";
    private static final String NUM_RECORDS = "NUM_RECORDS";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private CheckpointService checkpointService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        log.info("TenantId=" + MultiTenantContext.getTenant().getId());
    }

    @AfterClass(groups = "deployment")
    protected void cleanup() {
        checkpointService.cleanup();
    }

    @Test(groups = "deployment", priority = 0)
    public void testOrphanTransactionExport() throws Exception {
        checkpointService.resumeCheckpoint(CHECK_POINT, 19);
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.TRANSACTION);
        log.info("OrphanRecordsExportRequest=" + JsonUtils.serialize(request));

        ApplicationId appid = cdlProxy.OrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId=" + appid.toString());

        JobStatus status = waitForWorkflowStatus(appid.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);

        Map<String, Integer> expectedResults = new HashMap<>();
        expectedResults.put(NUM_FILES, 5);
        expectedResults.put(NUM_RECORDS, 17218);
        verifyResults(expectedResults);
    }

    @Test(groups = "deployment", priority = 1)
    public void testOrphanContactExport() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.CONTACT);
        log.info("OrphanRecordsExportRequest=" + JsonUtils.serialize(request));

        ApplicationId appid = cdlProxy.OrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId=" + appid.toString());

        JobStatus status = waitForWorkflowStatus(appid.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);

        Map<String, Integer> expectedResults = new HashMap<>();
        expectedResults.put(NUM_FILES, 5);
        expectedResults.put(NUM_RECORDS, 6);
        verifyResults(expectedResults);
    }

    @Test(groups = "deployment", priority = 2)
    public void testUnmatchedAccountExport() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        OrphanRecordsExportRequest request = createExportJob(OrphanRecordsType.UNMATCHED_ACCOUNT);
        log.info("OrphanRecordsExportRequest=" + JsonUtils.serialize(request));

        ApplicationId appid = cdlProxy.OrphanRecordsExport(customerSpace, request);
        log.info("ApplicationId=" + appid.toString());

        JobStatus status = waitForWorkflowStatus(appid.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);

        Map<String, Integer> expectedResults = new HashMap<>();
        expectedResults.put(NUM_FILES, 5);
        expectedResults.put(NUM_RECORDS, 18);
        verifyResults(expectedResults);
    }

    private void verifyResults(Map<String, Integer> expectedResults) {
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        String dir = String.format("/Pods/Default/Contracts/%s/Tenants/%s/Spaces/Production/Data/Files/Exports",
                tenantId, tenantId);
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dir,".*.csv$");
            Assert.assertNotNull(files);
            Assert.assertEquals(files.size(), expectedResults.get(NUM_FILES).intValue());
            int totalRecordNum = 0;
            for (String file : files) {
                CSVParser parser = new CSVParser(
                        new InputStreamReader(HdfsUtils.getInputStream(yarnConfiguration, file)), LECSVFormat.format);
                List<CSVRecord> records = parser.getRecords();
                totalRecordNum += records.size();
            }
            Assert.assertEquals(totalRecordNum, expectedResults.get(NUM_RECORDS).intValue());
        } catch (IOException e) {
            e.printStackTrace();
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
