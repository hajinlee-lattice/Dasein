package com.latticeengines.apps.dcp.end2end;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Date;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.impl.DataReportServiceImplTestNG;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadRequest;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class DCPDataReportWorkflowDeploymentTestNG extends DCPDeploymentTestNGBase {

    private ProjectDetails projectDetails;
    private Source source;
    private Source source1;
    private DataReport report;
    private DataReport report1;
    private DataReport report2;
    private DataReport report3;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DataReportProxy reportProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private Configuration yarnConfiguration;

    private static final String localPathBase = ClassLoader
            .getSystemResource("avro").getPath();

    @BeforeClass(groups = { "deployment" })
    public void setup() throws IOException {
        setupTestEnvironment();
        prepareTenant();
    }

    @Test(groups = "deployment")
    public void testRecomputeRoot() {
        // two uploads in source
        DCPReportRequest request = new DCPReportRequest();
        request.setLevel(DataReportRecord.Level.Source);
        request.setMode(DataReportMode.RECOMPUTE_ROOT);
        request.setRootId(source.getSourceId());

        ApplicationId appId = reportProxy.rollupDataReport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);

        DataReport sourceReport = reportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Source,
                source.getSourceId());


        System.out.println(JsonUtils.pprint(report));
        System.out.println(JsonUtils.pprint(report1));
        System.out.println("Source report: ");
        System.out.println(JsonUtils.pprint(sourceReport));
        DataReport initialReport = initializeReport();
        initialReport.combineReport(report);
        initialReport.combineReport(report1);
        Assert.assertEquals(JsonUtils.pprint(initialReport.getBasicStats()),
                JsonUtils.pprint(sourceReport.getBasicStats()));
        Assert.assertEquals(JsonUtils.pprint(initialReport.getGeoDistributionReport()),
                JsonUtils.pprint(sourceReport.getGeoDistributionReport()));
        Assert.assertEquals(JsonUtils.pprint(initialReport.getInputPresenceReport()),
                JsonUtils.pprint(sourceReport.getInputPresenceReport()));
        // dup report is relative to the duns count table, which will be generated in set up phase
        DataReport.DuplicationReport dupReport = sourceReport.getDuplicationReport();
        Assert.assertNotNull(sourceReport);
        Assert.assertEquals(dupReport.getDuplicateRecords(), Long.valueOf(16));
        Assert.assertEquals(dupReport.getDistinctRecords(), Long.valueOf(8L));
        Assert.assertEquals(dupReport.getUniqueRecords(), Long.valueOf(0L));
    }

    @Test(groups = "deployment", dependsOnMethods = "testRecomputeRoot")
    public void testRecomputeTree() {
        DCPReportRequest request = new DCPReportRequest();
        request.setLevel(DataReportRecord.Level.Source);
        request.setMode(DataReportMode.RECOMPUTE_TREE);
        request.setRootId(source1.getSourceId());

        ApplicationId appId = reportProxy.rollupDataReport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);

        DataReport sourceReport = reportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Source,
                source1.getSourceId());

        System.out.println(JsonUtils.pprint(report2));
        System.out.println(JsonUtils.pprint(report3));
        System.out.println("Source report");
        System.out.println(JsonUtils.pprint(sourceReport));
        DataReport initialReport = initializeReport();
        initialReport.combineReport(report2);
        initialReport.combineReport(report3);
        Assert.assertEquals(JsonUtils.pprint(initialReport.getBasicStats()),
                JsonUtils.pprint(sourceReport.getBasicStats()));
        Assert.assertEquals(JsonUtils.pprint(initialReport.getGeoDistributionReport()),
                JsonUtils.pprint(sourceReport.getGeoDistributionReport()));
        Assert.assertEquals(JsonUtils.pprint(initialReport.getInputPresenceReport()),
                JsonUtils.pprint(sourceReport.getInputPresenceReport()));
        // dup report is relative to the duns count table, which will be generated in set up phase
        DataReport.DuplicationReport dupReport = sourceReport.getDuplicationReport();
        Assert.assertNotNull(dupReport);
        Assert.assertEquals(dupReport.getDuplicateRecords(), Long.valueOf(16));
        Assert.assertEquals(dupReport.getDistinctRecords(), Long.valueOf(8L));
        Assert.assertEquals(dupReport.getUniqueRecords(), Long.valueOf(0L));
    }

    @Test(groups = "deployment", dependsOnMethods = "testRecomputeTree")
    public void testUpdate() {
        DCPReportRequest request = new DCPReportRequest();
        request.setLevel(DataReportRecord.Level.Project);
        request.setMode(DataReportMode.UPDATE);
        request.setRootId(projectDetails.getProjectId());

        ApplicationId appId = reportProxy.rollupDataReport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);

        // verify the project report only are generated from source report and source1 report
        DataReport projectReport = reportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Project,
                projectDetails.getProjectId());

        DataReport sourceReport = reportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Source,
                source.getSourceId());
        DataReport sourceReport1 = reportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Source,
                source1.getSourceId());
        System.out.println(JsonUtils.pprint(projectReport));
        DataReport initialReport = initializeReport();
        initialReport.combineReport(sourceReport);
        initialReport.combineReport(sourceReport1);
        Assert.assertEquals(JsonUtils.pprint(initialReport.getBasicStats()), JsonUtils.pprint(projectReport.getBasicStats()));
        Assert.assertEquals(JsonUtils.pprint(initialReport.getGeoDistributionReport()),
                JsonUtils.pprint(projectReport.getGeoDistributionReport()));
        Assert.assertEquals(JsonUtils.pprint(initialReport.getInputPresenceReport()),
                JsonUtils.pprint(projectReport.getInputPresenceReport()));
        // dup report is relative to the duns count table, which will be generated in set up phase
        DataReport.DuplicationReport dupReport = projectReport.getDuplicationReport();
        Assert.assertNotNull(dupReport);
        Assert.assertEquals(dupReport.getDuplicateRecords(), Long.valueOf(32));
        Assert.assertEquals(dupReport.getDistinctRecords(), Long.valueOf(8L));
        Assert.assertEquals(dupReport.getUniqueRecords(), Long.valueOf(0L));
    }

    private void prepareTenant() throws IOException {
        // Create Project
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName("ReportEnd2EndProject");
        projectRequest.setProjectType(Project.ProjectType.Type1);
        projectDetails = projectProxy.createDCPProject(mainCustomerSpace, projectRequest, "dcp_deployment@dnb.com");
        // Create Source
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName("ReportEnd2EndSource");
        sourceRequest.setProjectId(projectDetails.getProjectId());
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        source = sourceProxy.createSource(mainCustomerSpace, sourceRequest);

        // create upload in source, create two uploads in source
        UploadRequest uploadRequest = generateUpload(projectDetails, source);
        UploadDetails uploadDetails = uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), uploadRequest);
        UploadDetails uploadDetails1 = uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), uploadRequest);


        // create another source in project, create two uploads in this source
        SourceRequest sourceRequest1 = new SourceRequest();
        sourceRequest1.setDisplayName("ReportEnd2EndSource1");
        sourceRequest1.setProjectId(projectDetails.getProjectId());
        sourceRequest1.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        source1 = sourceProxy.createSource(mainCustomerSpace, sourceRequest1);
        UploadDetails uploadDetails2 = uploadProxy.createUpload(mainCustomerSpace, source1.getSourceId(),
                uploadRequest);
        UploadDetails uploadDetails3 = uploadProxy.createUpload(mainCustomerSpace, source1.getSourceId(),
                uploadRequest);


        // register report in upload level
        report = DataReportServiceImplTestNG.getDataReport();
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails.getUploadId(),
                report);
        report1 = DataReportServiceImplTestNG.getDataReport();
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails1.getUploadId(),
                report1);
        report2 = DataReportServiceImplTestNG.getDataReport();
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails2.getUploadId(),
                report2);
        report3 = DataReportServiceImplTestNG.getDataReport();
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails3.getUploadId(),
                report3);

        // register duns count table for different level
        String tableName = setupTables();
        DunsCountCache cache = new DunsCountCache();
        cache.setSnapshotTimestamp(new Date());
        cache.setDunsCountTableName(tableName);

        reportProxy.registerDunsCount(mainCustomerSpace, DataReportRecord.Level.Project,
                projectDetails.getProjectId(), cache);
        SleepUtils.sleep(1000);
        cache.setSnapshotTimestamp(new Date());
        reportProxy.registerDunsCount(mainCustomerSpace, DataReportRecord.Level.Source, source.getSourceId(),
                cache);

        SleepUtils.sleep(1000);
        cache.setSnapshotTimestamp(new Date());
        reportProxy.registerDunsCount(mainCustomerSpace, DataReportRecord.Level.Source, source1.getSourceId(),
                cache);

        SleepUtils.sleep(1000);
        cache.setSnapshotTimestamp(new Date());
        reportProxy.registerDunsCount(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails.getUploadId(),
                cache);
        DataReport reportBeforeSetReadyForRollUp = reportProxy.getReadyForRollUpDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails.getUploadId());
        Assert.assertNull(reportBeforeSetReadyForRollUp);
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails.getUploadId());
        reportBeforeSetReadyForRollUp = reportProxy.getReadyForRollUpDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails.getUploadId());
        Assert.assertNotNull(reportBeforeSetReadyForRollUp);

        SleepUtils.sleep(1000);
        cache.setSnapshotTimestamp(new Date());
        reportProxy.registerDunsCount(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails1.getUploadId(),
                cache);
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails1.getUploadId());

        SleepUtils.sleep(1000);
        cache.setSnapshotTimestamp(new Date());
        reportProxy.registerDunsCount(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails2.getUploadId(),
                cache);
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails2.getUploadId());

        SleepUtils.sleep(1000);
        cache.setSnapshotTimestamp(new Date());
        reportProxy.registerDunsCount(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails3.getUploadId(),
                cache);
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails3.getUploadId());
    }

    private String setupTables() throws IOException {
        Table dunsCountTable = JsonUtils
                .deserialize(IOUtils.toString(ClassLoader.getSystemResourceAsStream(
                        "tables/DunsCountTable.json"), "UTF-8"), Table.class);
        String dunsCountTableName = NamingUtils.timestamp("dunsCount");
        dunsCountTable.setName(dunsCountTableName);
        Extract extract = dunsCountTable.getExtracts().get(0);
        extract.setPath(PathBuilder
                .buildDataTablePath(CamilleEnvironment.getPodId(),
                        CustomerSpace.parse(mainCustomerSpace))
                .append(dunsCountTableName).toString()
                + "/*.avro");
        dunsCountTable.setExtracts(Collections.singletonList(extract));
        metadataProxy.createTable(mainCustomerSpace, dunsCountTableName, dunsCountTable);

        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, //
                  localPathBase + "/part1.avro", //
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(),
                                CustomerSpace.parse(mainCustomerSpace))
                        .append(dunsCountTableName).append("part1.avro").toString());
        return dunsCountTableName;
    }

    private UploadRequest generateUpload(ProjectDetails details, Source source) {
        UploadRequest uploadRequest = new UploadRequest();
        UploadConfig config = new UploadConfig();
        uploadRequest.setUploadConfig(config);
        uploadRequest.setUserId("testReport");
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String uploadDir = UploadS3PathBuilderUtils.getUploadRoot(details.getProjectId(), source.getSourceId());
        String uploadDirKey = UploadS3PathBuilderUtils.combinePath(false, true, dropFolder, uploadDir);

        String uploadTS = "2020-07-16";
        String errorPath = uploadDirKey + uploadTS + "/error/file1.csv";
        String rawPath = uploadDirKey + uploadTS + "/raw/file2.csv";
        config.setUploadImportedErrorFilePath(errorPath);
        config.setUploadRawFilePath(rawPath);
        return uploadRequest;
    }

    private DataReport initializeReport() {
        DataReport report = new DataReport();
        report.setInputPresenceReport(new DataReport.InputPresenceReport());
        DataReport.BasicStats stats = new DataReport.BasicStats();
        stats.setSuccessCnt(0L);
        stats.setErrorCnt(0L);
        stats.setPendingReviewCnt(0L);
        stats.setMatchedCnt(0L);
        stats.setTotalSubmitted(0L);
        stats.setUnmatchedCnt(0L);
        report.setBasicStats(stats);
        report.setDuplicationReport(new DataReport.DuplicationReport());
        report.setGeoDistributionReport(new DataReport.GeoDistributionReport());
        report.setMatchToDUNSReport(new DataReport.MatchToDUNSReport());
        return report;
    }
}
