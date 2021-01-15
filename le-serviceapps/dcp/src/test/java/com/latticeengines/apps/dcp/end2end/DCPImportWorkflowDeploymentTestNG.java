package com.latticeengines.apps.dcp.end2end;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.WorkflowJobService;
import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.datacloud.MatchCoreErrorConstants;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DownloadFileType;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.proxy.exposed.matchapi.UsageProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

import au.com.bytecode.opencsv.CSVReader;

public class DCPImportWorkflowDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DCPImportWorkflowDeploymentTestNG.class);

    private static final String TEST_DATA_DIR = "le-serviceapps/dcp/deployment/testdata";
    private static final String TEST_DATA_VERSION = "6";
    private static final String TEST_ACCOUNT_DATA_FILE = "Account_1_900.csv";
    private static final String TEST_ACCOUNT_ERROR_FILE = "Account_dup_header.csv";
    private static final String TEST_ACCOUNT_MISSING_REQUIRED = "Account_missing_country.csv";

    protected static final String TEST_TEMPLATE_VERSION = "4";

    private static final String USER = "test@dnb.com";

    public static final List<String> MATCH_COLUMNS = Arrays.asList(
            "Matched D-U-N-S Number",
            "Match Type",
            "Match Confidence Code",
            "Match Grade",
            "Match Data Profile",
            "Name Match Score",
            "Match Candidate Operating Status",
            "Match Primary Business Name",
            "Match ISO Alpha 2 Char Country Code"
    );

    public static final List<String> BASE_COLUMNS = Arrays.asList(
            "D-U-N-S Number", //
            "Primary Business Name", //
            "ISO Alpha 2 Char Country Code" //
    );

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataReportProxy dataReportProxy;

    @Inject
    private UsageProxy usageProxy;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Inject
    private S3Service s3Service;

    @Inject
    private UploadService uploadService;

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    private WorkflowJobService workflowJobService;

    private ProjectDetails projectDetails;
    private Source source;
    private String uploadId;
    private String s3FileKey;

    @BeforeClass(groups = { "deployment" })
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void testImport() {
        prepareTenant();
        setEnrichmentLayout();

        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(projectDetails.getProjectId());
        request.setSourceId(source.getSourceId());
        request.setS3FileKey(s3FileKey);
        request.setUserId(USER);
        ApplicationId applicationId = uploadProxy.startImport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);

        List<UploadDetails> uploadList = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null,
                Boolean.FALSE, 0, 20);
        Assert.assertNotNull(uploadList);
        Assert.assertEquals(uploadList.size(), 1);
        UploadDetails upload = uploadList.get(0);
        uploadId = upload.getUploadId();

        verifyImport(false);

        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "deployment", dependsOnMethods = "testImport")
    public void testErrorImport() {

        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropPath = UploadS3PathBuilderUtils.getDropRoot(projectDetails.getProjectId(), source.getSourceId());
        dropPath = UploadS3PathBuilderUtils.combinePath(false, true,
                UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox()), dropPath);
        String errorFileKey = dropPath + TEST_ACCOUNT_ERROR_FILE;
        testArtifactService.copyTestArtifactFile(TEST_DATA_DIR, TEST_DATA_VERSION, TEST_ACCOUNT_ERROR_FILE, s3Bucket,
                errorFileKey);

        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(projectDetails.getProjectId());
        request.setSourceId(source.getSourceId());
        request.setS3FileKey(errorFileKey);
        ApplicationId applicationId = uploadProxy.startImport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.FAILED);
        List<UploadDetails> uploadList = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null,
                Boolean.FALSE, 0, 20);
        Assert.assertNotNull(uploadList);
        Assert.assertEquals(uploadList.size(), 2);
        UploadDetails upload = uploadList.get(0).getUploadId().equals(uploadId) ? uploadList.get(1) : uploadList.get(0);
        Assert.assertEquals(upload.getStatus(), Upload.Status.ERROR);
        Assert.assertNotNull(upload.getUploadDiagnostics().getApplicationId());
        Assert.assertNotNull(upload.getUploadDiagnostics().getLastErrorMessage());
    }

    @Test(groups = "deployment", dependsOnMethods = "testImport")
    public void testProcessingErrors() {

        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(projectDetails.getProjectId());
        request.setSourceId(source.getSourceId());
        request.setS3FileKey(s3FileKey);
        request.setUserId(USER);
        request.setSuppressKnownMatchErrors(false);
        ApplicationId applicationId = uploadProxy.startImport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);

        List<UploadDetails> uploadList = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null,
                Boolean.FALSE, 0, 20);
        Assert.assertNotNull(uploadList);
        Assert.assertEquals(uploadList.size(), 3);

        Job submittedJob = workflowJobService.findByApplicationId(applicationId.toString());
        uploadId = submittedJob.getInputs().get(DCPSourceImportWorkflowConfiguration.UPLOAD_ID);
        log.info(uploadId);

        verifyImport(true);
    }

    @Test(groups = "deployment", dependsOnMethods = "testErrorImport")
    public void testMissingRequired() {

        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropPath = UploadS3PathBuilderUtils.getDropRoot(projectDetails.getProjectId(), source.getSourceId());
        dropPath = UploadS3PathBuilderUtils.combinePath(false, true,
                UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox()), dropPath);
        String errorFileKey = dropPath + TEST_ACCOUNT_MISSING_REQUIRED;
        testArtifactService.copyTestArtifactFile(TEST_DATA_DIR, TEST_DATA_VERSION, TEST_ACCOUNT_MISSING_REQUIRED, s3Bucket,
                errorFileKey);

        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(projectDetails.getProjectId());
        request.setSourceId(source.getSourceId());
        request.setS3FileKey(errorFileKey);
        ApplicationId applicationId = uploadProxy.startImport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.FAILED);
        List<UploadDetails> uploadList = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null,
                Boolean.TRUE, 0, 20);
        Assert.assertNotNull(uploadList);
        Assert.assertEquals(uploadList.size(), 4);
        UploadDetails upload = uploadList.get(0).getUploadId().equals(uploadId) ? uploadList.get(1) : uploadList.get(0);
        Assert.assertEquals(upload.getStatus(), Upload.Status.ERROR);
        Assert.assertNotNull(upload.getUploadDiagnostics().getApplicationId());
        System.out.println(upload.getUploadDiagnostics().getLastErrorMessage());
        Assert.assertNotNull(upload.getUploadDiagnostics().getLastErrorMessage());
        Assert.assertNotNull(upload.getUploadConfig().getUploadImportedErrorFilePath());
    }

    @Test(groups = "deployment", dependsOnMethods = "testMissingRequired")
    public void testHardDeleteProject() {

        projectProxy.hardDeleteProject(mainCustomerSpace, projectDetails.getProjectId(), null);

        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);

        Assert.assertFalse(s3Service.objectExist(dropBoxSummary.getBucket(), projectDetails.getProjectRootPath()));
        try {
            Assert.assertNull(sourceProxy.getSource(mainCustomerSpace, source.getSourceId()));
            Assert.assertTrue(CollectionUtils.isEmpty(uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null,
                    Boolean.FALSE, 0, 99)));
            DataReport report = dataReportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Project, projectDetails.getProjectId());
            Assert.assertNull(report);
            report = dataReportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Source, source.getSourceId());
            Assert.assertNull(report);
            report = dataReportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadId);
            Assert.assertNull(report);
        }catch (Exception e){
            log.info("got exception if source or upload have been deleted");
        }
    }

    private void setEnrichmentLayout() {
        EnrichmentLayout enrichmentLayout = new EnrichmentLayout();
        enrichmentLayout.setSourceId(source.getSourceId());
        enrichmentLayout.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        enrichmentLayout.setDomain(DataDomain.SalesMarketing);
        enrichmentLayout.setRecordType(DataRecordType.Domain);
        enrichmentLayout.setElements(Arrays.asList( //
                "primaryaddr_country_isoalpha2code", //
                "primaryaddr_country_name", //
                "primaryaddr_county_name", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_abbreviatedname", //
                "primaryaddr_continentalregion_name", //
//                "dnbassessment_marketingriskclass_desc", //
                "primaryaddr_isregisteredaddr", //
                "primaryaddr_language_desc", //
                "primaryaddr_language_code", //
                "primaryaddr_minortownname", //
                "primaryaddr_postalcode", //
                "primaryaddr_postalcodeposition_desc", //
                "primaryaddr_postalcodeposition_code", //
                "primaryaddr_postofficebox_postofficeboxnumber", //
                "primaryaddr_postofficebox_typedesc", //
                "primaryaddr_postofficebox_typecode", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_streetname", //
                "primaryaddr_streetnumber", //
                "primaryindcode_ussicv4", //
                "primaryindcode_ussicv4desc", //
                "primaryaddr_latitude", //
                "primaryaddr_longitude", //
                "numberofemployees_employeefiguresdate", //
                "registeredname", //
                "primaryname", //
//                "dnbassessment_marketingriskclass_code", //
                "duns_number", //
                "countryisoalpha2code" //
        ));
        ResponseDocument<String> response =  enrichmentLayoutService.create(mainCustomerSpace, enrichmentLayout);
        Assert.assertNotNull(response);
        Assert.assertTrue(response.isSuccess(), "EnrichmentLayout is not valid: " + JsonUtils.serialize(response));

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);
        EnrichmentLayout saved = retry.execute(ctx -> {
           EnrichmentLayout layout = enrichmentLayoutService.findBySourceId(mainCustomerSpace, source.getSourceId());
           Assert.assertNotNull(layout);
           return layout;
        });
        Assert.assertTrue(saved.getElements().contains("registeredname"));
    }

    private void verifyImport(boolean containsProcessingErrors) {
        UploadDetails upload = uploadProxy.getUploadByUploadId(mainCustomerSpace, uploadId, Boolean.TRUE);
        log.info(JsonUtils.serialize(upload));
        Assert.assertNotNull(upload);
        Assert.assertNotNull(upload.getStatus());

        Assert.assertEquals(upload.getDisplayName(), TEST_ACCOUNT_DATA_FILE);
        Assert.assertEquals(upload.getCreatedBy(), USER);
        Assert.assertEquals(upload.getStatus(), Upload.Status.FINISHED);
        Assert.assertNotNull(upload.getUploadDiagnostics().getApplicationId());
        Assert.assertNull(upload.getUploadDiagnostics().getLastErrorMessage());

        Assert.assertNotNull(upload.getDropFileTime());
        Assert.assertTrue(upload.getDropFileTime() > 1590000000000L);
        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getDropFilePath()));
        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getUploadRawFilePath()));
        // Only verify the Error File if there are errors during ingestion and thus the file exists.
        if (upload.getStatistics().getImportStats().getFailedIngested() > 0) {
            System.out.println("Found " + upload.getStatistics().getImportStats().getFailedIngested() +
                    " errors.  Verifying error file.");
            Assert.assertTrue(upload.getUploadConfig().getDownloadableFiles().contains(DownloadFileType.IMPORT_ERRORS));
            verifyErrorFile(upload);
        } else {
            Assert.assertFalse(upload.getUploadConfig().getDownloadableFiles().contains(DownloadFileType.IMPORT_ERRORS));
            System.out.println("No ingestion errors found, skipping error file validation.");
        }

        Assert.assertEquals(upload.getUploadConfig().getDownloadableFiles().contains(DownloadFileType.PROCESS_ERRORS), containsProcessingErrors);

        verifyMatchResult(upload, containsProcessingErrors);
        verifyUploadStats(upload);
        verifyDownload(upload);
        verifyDataReport();
        // verifyUsageReport(upload);
    }

    private void verifyErrorFile(UploadDetails upload) {
        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getUploadImportedErrorFilePath()));
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String errorFileKey = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                upload.getUploadConfig().getUploadImportedErrorFilePath());
        System.out.println("Error file path=" + errorFileKey);
        Assert.assertTrue(s3Service.objectExist(dropBoxSummary.getBucket(), errorFileKey));
        Assert.assertNotNull(upload.getStatistics().getImportStats().getFailedIngested());
        // There are 10 error rows.  4 records are missing a Company Name.  2 records are missing a Country.
        // 2 records are missing both Company Name and Country.  2 records are blank.
        Assert.assertEquals(upload.getStatistics().getImportStats().getFailedIngested().longValue(), 10L);
    }

    private void prepareTenant() {
        // Create Project
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName("ImportEnd2EndProject");
        projectRequest.setProjectType(Project.ProjectType.Type1);
        projectRequest.setPurposeOfUse(getPurposeOfUse());
        projectDetails = projectProxy.createDCPProject(mainCustomerSpace, projectRequest, "dcp_deployment@dnb.com");
        // Add customer fields to base spec
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        String[] fieldNames = {"user_Total_Employees", "user_MarketoAccountID", "user_Sales_in__B"};
        String[] columnNames = {"Total Employees", "MarketoAccountID", "Sales in $B"};
        addCustomerFields(fieldNames, columnNames, fieldDefinitionsRecord);
        // Create Source
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName("ImportEnd2EndSource");
        sourceRequest.setProjectId(projectDetails.getProjectId());
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        source = sourceProxy.createSource(mainCustomerSpace, sourceRequest);
        // Pause this source for s3 import.
        sourceProxy.pauseSource(mainCustomerSpace, source.getSourceId());
        // Copy test file to drop folder
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropPath = UploadS3PathBuilderUtils.getDropRoot(projectDetails.getProjectId(), source.getSourceId());
        dropPath = UploadS3PathBuilderUtils.combinePath(false, true,
                UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox()), dropPath);
        s3FileKey = dropPath + TEST_ACCOUNT_DATA_FILE;
        testArtifactService.copyTestArtifactFile(TEST_DATA_DIR, TEST_DATA_VERSION, TEST_ACCOUNT_DATA_FILE, s3Bucket,
                s3FileKey);
    }

    private void addCustomerFields(String[] fields, String[] columns, FieldDefinitionsRecord record) {
        for (int i = 0; i < fields.length; i++) {
            FieldDefinition fd = new FieldDefinition();
            fd.setFieldName(fields[i]);
            fd.setFieldType(UserDefinedType.TEXT);
            fd.setColumnName(columns[i]);
            fd.setInCurrentImport(false);
            fd.setRequired(false);
            fd.setNullable(true);
            fd.setMappedToLatticeId(false);
            record.addFieldDefinition("Custom Fields", fd, false);
        }
    }

    private void verifyUploadStats(UploadDetails upload) {
        UploadStats uploadStats = upload.getStatistics();
        System.out.println(JsonUtils.serialize(uploadStats));
        Assert.assertNotNull(uploadStats);
        UploadStats.ImportStats importStats = uploadStats.getImportStats();
        Assert.assertNotNull(importStats);
        Assert.assertEquals(importStats.getSuccessfullyIngested().longValue(), 20L);

        UploadStats.MatchStats matchStats = uploadStats.getMatchStats();
        Assert.assertNotNull(matchStats);
        Assert.assertTrue(matchStats.getMatched() > 0);

        Assert.assertEquals(Long.valueOf(matchStats.getMatched() + //
                matchStats.getPendingReviewCnt() + matchStats.getUnmatched()), importStats.getSuccessfullyIngested());
    }

    private void verifyMatchResult(UploadDetails upload, boolean containsProcessingErrors) {
        String uploadId = upload.getUploadId();
        String matchResultName = uploadService.getMatchResultTableName(mainCustomerSpace, uploadId);
        Assert.assertNotNull(matchResultName);
        Table matchResult = metadataProxy.getTableSummary(mainCustomerSpace, matchResultName);
        Assert.assertNotNull(matchResult);
        Assert.assertEquals(matchResult.getExtracts().size(), 1);

        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String acceptedPath = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                upload.getUploadConfig().getUploadMatchResultAccepted());
        String rejectedPath = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                upload.getUploadConfig().getUploadMatchResultRejected());
        String bucket = dropBoxSummary.getBucket();
        System.out.println("acceptedPath=" + acceptedPath);
        System.out.println("rejectedPath=" + rejectedPath);
        Assert.assertTrue(s3Service.objectExist(bucket, acceptedPath));
        Assert.assertTrue(s3Service.objectExist(bucket, rejectedPath));
        verifyCsvContent(bucket, acceptedPath, false);
        verifyCsvContent(bucket, rejectedPath, true);

        if (containsProcessingErrors) {
            verifyProcessingErrors(bucket, acceptedPath, false);

            String processingErrorsPath = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                    upload.getUploadConfig().getUploadMatchResultErrored());
            System.out.println("processing_errors=" + processingErrorsPath);
            Assert.assertTrue(s3Service.objectExist(bucket, processingErrorsPath));
            verifyCsvContent(bucket, processingErrorsPath, true);
            verifyProcessingErrors(bucket, processingErrorsPath, true);
        }
    }

    private void verifyCsvContent(String bucket, String path, boolean containsProcessingErrors) {
        InputStream is = s3Service.readObjectAsStream(bucket, path);
        InputStreamReader reader = new InputStreamReader(is);
        try (CSVReader csvReader = new CSVReader(reader)) {
            String[] nextRecord = csvReader.readNext();
            int count = 0;
            List<String> headers = new ArrayList<>();

            int idIdx = -1;
            int nameIdx = -1;
            int dunsIdx = -1;
            int ccIdx = -1;
            int rnIdx = -1;
            int mtIdx = -1;
            int mceIdx = -1;
            int mcecIdx = -1;
            int mceiIdx = -1;

            while (nextRecord != null && (count++) < 100) {
                if (count == 1) {
                    headers.addAll(Arrays.asList(nextRecord));
                    verifyOutputHeaders(headers, containsProcessingErrors);
                    idIdx = headers.indexOf("Customer ID");
                    nameIdx = headers.indexOf("Company Name");
                    dunsIdx = headers.indexOf("Matched D-U-N-S Number");
                    ccIdx = headers.indexOf("Match Confidence Code");
                    rnIdx = headers.indexOf("Registration Number");
                    mtIdx = headers.indexOf("Match Type");
                    mceIdx = headers.indexOf("Processing Error Type");
                    mcecIdx = headers.indexOf("Processing Error Code");
                    Assert.assertTrue(idIdx < dunsIdx);
                    Assert.assertTrue(dunsIdx < ccIdx);
                    Assert.assertEquals(containsProcessingErrors, mceIdx != -1);
                    if (containsProcessingErrors) {
                        Assert.assertEquals(headers.get(mceIdx + 1), "Processing Error Code");
                        Assert.assertEquals(headers.get(mceIdx + 2), "Processing Error Details");
                    }
                } else {
                    String customerId = nextRecord[idIdx];
                    String matchType = nextRecord[mtIdx];
                    if ("25".equals(customerId)) { // url
                        log.info("CSV record for [bp.com]: {}", StringUtils.join(nextRecord, ","));
                        Assert.assertEquals(matchType, "Domain Lookup");
                    } else if (Arrays.asList("21", "22", "23", "24").contains(customerId)) { // reg number
                        String regNumber = nextRecord[rnIdx];
                        Assert.assertNotNull(regNumber);
                        String matchDuns = nextRecord[dunsIdx];
                        log.info("CSV record for [{}]: {}", regNumber, StringUtils.join(nextRecord, ","));
                        switch (regNumber) {
                            case "432126092":
                                Assert.assertEquals(matchDuns, "268487989");
                                Assert.assertEquals(matchType, "National ID Lookup");
                                break;
                            case "DE129273398":
                                Assert.assertEquals(matchDuns, "315369934");
                                Assert.assertEquals(matchType, "National ID Lookup");
                                break;
                            case "77-0493581":
                                Assert.assertEquals(matchDuns, "060902413");
                                Assert.assertEquals(matchType, "National ID Lookup");
                                break;
                            case "160043":
                                Assert.assertEquals(matchDuns, "229515499");
                                Assert.assertEquals(matchType, "National ID Lookup");
                                break;
                            default:
                        }
                    } else if ("4".equals(customerId)) { // matched but rejected
                        String companyName = nextRecord[nameIdx];
                        Assert.assertEquals(companyName, "AAC Technologies Holdings");
                        log.info("CSV record for [Tencent]: {}", StringUtils.join(nextRecord, ","));
                        String confidenceCode = nextRecord[ccIdx];
                        Assert.assertTrue(StringUtils.isNotBlank(confidenceCode));
                        Assert.assertTrue(Integer.parseInt(confidenceCode) < 6);
                    } else {
                        String companyName = nextRecord[nameIdx];

                        if ("000\"".equals(companyName)) {
                            Assert.assertEquals(nextRecord[mceIdx], MatchCoreErrorConstants.ErrorType.MATCH_ERROR.name());
                            Assert.assertEquals(nextRecord[mcecIdx], "10002");
                        }
                        Assert.assertTrue(StringUtils.isNotBlank(companyName)); // Original Name is non-empty
                    }
                    nextRecord = csvReader.readNext();
                }
            }
        } catch (IOException e) {
            Assert.fail("Failed to read output csv", e);
        }
    }

    private void verifyProcessingErrors(String bucket, String path, boolean shouldContainErrors) {

        log.info(path);
        InputStream is = s3Service.readObjectAsStream(bucket, path);
        InputStreamReader reader = new InputStreamReader(is);
        try (CSVReader csvReader = new CSVReader(reader)) {
            String[] nextRecord = csvReader.readNext();
            int count = 0;
            List<String> headers = new ArrayList<>();

            int mceIdx = -1;
            int mcecIdx = -1;

            while (nextRecord != null && (count++) < 100) {
                if (count == 1) {
                    headers.addAll(Arrays.asList(nextRecord));
                    verifyOutputHeaders(headers, shouldContainErrors);
                    mceIdx = headers.indexOf("Processing Error Type");
                    mcecIdx = headers.indexOf("Processing Error Code");
                } else if (shouldContainErrors) {
                    Assert.assertTrue(StringUtils.isNotEmpty(nextRecord[mceIdx]));
                    Assert.assertTrue(StringUtils.isNotEmpty(nextRecord[mcecIdx]));
                }

                nextRecord = csvReader.readNext();
            }
        } catch (IOException e) {
            Assert.fail("Failed to read output csv", e);
        }
    }

    private void verifyOutputHeaders(List<String> headers, boolean containsProcessingErrors) {
        System.out.println(headers);

        String[] expectedHeaders = {
                "Customer ID", "Website", "Company Name", "City", "State",
                "Country", "Postal Code", "Registration Number", "Sales in $B", "Total Employees", "MarketoAccountID",
                "Street Address Line 1", "Street Address Line 2", "Phone Number", "DUNS Number"
        };

        for (int i = 0; i < expectedHeaders.length; i++)
            Assert.assertEquals(headers.indexOf(expectedHeaders[i]), i);

        Assert.assertFalse(headers.contains("Test Date 2")); // not in spec
        Assert.assertFalse(headers.contains("__Matched_DUNS__")); // debug column
        Assert.assertFalse(headers.contains("__Matched_Name__")); // debug column
        if (headers.contains("D-U-N-S Number")) { // the accepted csv
            Assert.assertTrue(headers.contains("Registered Name")); // in enrichment layout
            Assert.assertTrue(headers.contains("Primary Address Region Abreviated Name"),"Header should contain 'Primary Address Region Abreviated Name'."); // in enrichment layout (The spelling of Abreviated comes from the D+ dataDictionary)
            Assert.assertFalse(headers.contains("Primary Address Region Name")); // not in enrichment layout
            Assert.assertTrue(headers.contains("Matched D-U-N-S Number"));

            // verify match/candidate column order
            int start = headers.indexOf("Matched D-U-N-S Number");
            Assert.assertEquals(start, containsProcessingErrors ? 18 : 15, "'Matched D-U-N-S Number' header is in the wrong position.  Headers are " + headers);
            Assert.assertEquals(headers.subList(start, start + MATCH_COLUMNS.size()), MATCH_COLUMNS);

            // verify base info column order
            start = headers.indexOf("D-U-N-S Number");
            Assert.assertEquals(start, containsProcessingErrors ? 27 : 24);
            Assert.assertEquals(headers.subList(start, start + BASE_COLUMNS.size()), BASE_COLUMNS);
        }

        MatchCoreErrorConstants.CSV_HEADER_MAP.values().forEach(errorHeader -> Assert.assertEquals(headers.contains(errorHeader), containsProcessingErrors));
    }

    private void verifyDownload(UploadDetails upload) {
        RestTemplate template = testBed.getRestTemplate();
        String tokenUrl = String.format("%s/pls/uploads/uploadId/%s/token", deployedHostPort,
                upload.getUploadId());
        String token = template.getForObject(tokenUrl, String.class);
        SleepUtils.sleep(300);
        String downloadUrl = String.format("%s/pls/filedownloads/%s", deployedHostPort, token);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.ALL));
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = template.exchange(downloadUrl, HttpMethod.GET, entity, byte[].class);
        String fileName = response.getHeaders().getFirst("Content-Disposition");
        Assert.assertTrue(StringUtils.isNotBlank(fileName) && fileName.contains(".zip"));
        byte[] contents = response.getBody();
        Assert.assertNotNull(contents);
        Assert.assertTrue(contents.length > 0);

    }

    private void verifyDataReport() {
        DataReport report = dataReportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadId);
        Assert.assertNotNull(report);
        System.out.println(JsonUtils.serialize(report));
        DataReport.BasicStats basicStats  = report.getBasicStats();
        Assert.assertNotNull(basicStats);
        Assert.assertTrue(basicStats.getSuccessCnt() > 0);
        Assert.assertTrue(basicStats.getMatchedCnt() > 0);
        Assert.assertEquals(Long.valueOf(basicStats.getMatchedCnt() + //
                basicStats.getPendingReviewCnt() + basicStats.getUnmatchedCnt()), basicStats.getSuccessCnt());

        DataReport.InputPresenceReport inputPresenceReport  = report.getInputPresenceReport();
        Assert.assertNotNull(inputPresenceReport);
        Assert.assertTrue(CollectionUtils.isNotEmpty(inputPresenceReport.getPresenceList()));

        DataReport.GeoDistributionReport geoDistributionReport = report.getGeoDistributionReport();
        Assert.assertNotNull(geoDistributionReport);
        Assert.assertTrue(CollectionUtils.isNotEmpty(geoDistributionReport.getGeographicalDistributionList()));

        DataReport.DuplicationReport duplicationReport = report.getDuplicationReport();
        Assert.assertNotNull(duplicationReport);
        Assert.assertTrue(duplicationReport.getDistinctRecords() > 0);

        DataReport.MatchToDUNSReport matchToDUNSReport = report.getMatchToDUNSReport();
        Assert.assertNotNull(matchToDUNSReport);

        DunsCountCache uploadCache = dataReportProxy.getDunsCount(mainCustomerSpace, DataReportRecord.Level.Upload
                , uploadId);
        Assert.assertNotNull(uploadCache);
        Assert.assertNotNull(uploadCache.getSnapshotTimestamp());
        Assert.assertNotNull(uploadCache.getDunsCountTableName());
        System.out.println(JsonUtils.pprint(uploadCache));

        DunsCountCache sourceCache = dataReportProxy.getDunsCount(mainCustomerSpace, DataReportRecord.Level.Source,
                source.getSourceId());
        Assert.assertNotNull(sourceCache);
        Assert.assertNotNull(sourceCache.getSnapshotTimestamp());
        Assert.assertNotNull(sourceCache.getDunsCountTableName());

        DunsCountCache projectCache = dataReportProxy.getDunsCount(mainCustomerSpace, DataReportRecord.Level.Project,
                projectDetails.getProjectId());
        Assert.assertNotNull(projectCache);
        Assert.assertNotNull(projectCache.getSnapshotTimestamp());
        Assert.assertNotNull(projectCache.getDunsCountTableName());

        DunsCountCache tenantCache = dataReportProxy.getDunsCount(mainCustomerSpace, DataReportRecord.Level.Tenant,
                CustomerSpace.parse(mainCustomerSpace).toString());
        Assert.assertNotNull(tenantCache);
        Assert.assertNotNull(tenantCache.getSnapshotTimestamp());
        Assert.assertNotNull(tenantCache.getDunsCountTableName());

        DataReport tenantReport = dataReportProxy.getReadyForRollupDataReport(mainCustomerSpace,
                DataReportRecord.Level.Tenant,
                CustomerSpace.parse(mainCustomerSpace).toString());
        Assert.assertNotNull(tenantReport);
        // test archive the project, then verify no child report in tenant level
        projectProxy.deleteProject(mainCustomerSpace, projectDetails.getProjectId(), null);
        Set<String> childrenIds = dataReportProxy.getChildrenIds(mainCustomerSpace, DataReportRecord.Level.Tenant,
                mainCustomerSpace);
        Assert.assertTrue(CollectionUtils.isEmpty(childrenIds));

        // verify the corner case: ready for rollup is false for tenant after archiving project
        tenantReport = dataReportProxy.getReadyForRollupDataReport(mainCustomerSpace,
                DataReportRecord.Level.Tenant,
                CustomerSpace.parse(mainCustomerSpace).toString());
        Assert.assertNull(tenantReport);
    }

    void verifyUsageReport(UploadDetails upload) {
        String usageReportPath = upload.getUploadConfig().getUsageReportFilePath();
        Pattern pattern = Pattern.compile("s3://(?<bucket>[^/]+)/(?<prefix>.*)");
        Matcher matcher = pattern.matcher(usageReportPath);
        Assert.assertTrue(matcher.matches());
        String s3Bucket = matcher.group("bucket");
        String s3Prefix = matcher.group("prefix");
        log.info("Verifying usage report at {}, {}", s3Bucket, s3Prefix);
        List<String> usageReportFiles = s3Service.getFilesForDir(s3Bucket, s3Prefix);
        Assert.assertFalse(CollectionUtils.isEmpty(usageReportFiles));
        verifyUsageCsvContent(s3Bucket, usageReportFiles.get(0));
    }

    private void verifyUsageCsvContent(String bucket, String path) {
        InputStream is = s3Service.readObjectAsStream(bucket, path);
        InputStreamReader reader = new InputStreamReader(is);
        try (CSVReader csvReader = new CSVReader(reader)) {
            String[] nextRecord = csvReader.readNext();
            int count = 0;
            List<String> headers = new ArrayList<>();
            while (nextRecord != null && (count++) < 100) {
                if (count == 1) {
                    headers.addAll(Arrays.asList(nextRecord));
                    verifyUsageOutputHeaders(headers);
                } else {
                    verifyUsageCsvRecord(nextRecord);
                    nextRecord = csvReader.readNext();
                }
            }
        } catch (IOException e) {
            Assert.fail("Failed to read output csv", e);
        }
    }

    private void verifyUsageOutputHeaders(List<String> headers) {
        System.out.println(headers);
        for (int i=0; i< headers.size(); i++) {
            Assert.assertEquals(VboUsageConstants.OUTPUT_FIELDS.get(i), headers.get(i));
        }
    }

    private void verifyUsageCsvRecord(String[] record) {
        System.out.println(StringUtils.join(record, ","));
        int drtIndex = VboUsageConstants.OUTPUT_FIELDS.indexOf(VboUsageConstants.ATTR_DRT);
        Assert.assertNotNull(record[drtIndex]);
        int poaeIdIndex = VboUsageConstants.OUTPUT_FIELDS.indexOf(VboUsageConstants.ATTR_POAEID);
        Assert.assertNotNull(record[poaeIdIndex]);
        int subscriberNumberIndex = VboUsageConstants.OUTPUT_FIELDS.indexOf(VboUsageConstants.ATTR_SUBSCRIBER_NUMBER);
        Assert.assertNotNull(record[subscriberNumberIndex]);
        int subjectDunsIndex = VboUsageConstants.OUTPUT_FIELDS.indexOf(VboUsageConstants.ATTR_SUBJECT_DUNS);
        Assert.assertNotNull(record[subjectDunsIndex]);
        int eventTypeIndex = VboUsageConstants.OUTPUT_FIELDS.indexOf(VboUsageConstants.ATTR_EVENT_TYPE);
        Assert.assertNotNull(record[eventTypeIndex]);
    }
}
