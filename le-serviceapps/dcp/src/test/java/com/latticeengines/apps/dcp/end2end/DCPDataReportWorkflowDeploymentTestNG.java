package com.latticeengines.apps.dcp.end2end;

import java.io.InputStream;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.impl.DataReportServiceImplTestNG;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadRequest;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;

public class DCPDataReportWorkflowDeploymentTestNG extends DCPDeploymentTestNGBase {

    private ProjectDetails projectDetails;
    private Source source;
    private Source source1;
    private DataReport report;
    private DataReport report1;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DataReportProxy reportProxy;

    @BeforeClass(groups = { "deployment" })
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void testRecomputeTree() {
        prepareTenant();
        DCPReportRequest request = new DCPReportRequest();
        request.setLevel(DataReportRecord.Level.Project);
        request.setMode(DataReportMode.RECOMPUTE_TREE);
        request.setRootId(projectDetails.getProjectId());

        ApplicationId appId = reportProxy.rollupDataReport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);

        DataReport projectReport = reportProxy.getDataReport(mainCustomerSpace, DataReportRecord.Level.Project,
                projectDetails.getProjectId());


        System.out.println(JsonUtils.pprint(report));
        System.out.println(JsonUtils.pprint(report1));
        System.out.println(JsonUtils.pprint(projectReport));
        report.combineReport(report1);
        Assert.assertEquals(JsonUtils.pprint(report.getBasicStats()), JsonUtils.pprint(projectReport.getBasicStats()));
        Assert.assertEquals(JsonUtils.pprint(report.getGeoDistributionReport()),
                JsonUtils.pprint(projectReport.getGeoDistributionReport()));
        Assert.assertEquals(JsonUtils.pprint(report.getInputPresenceReport()),
                JsonUtils.pprint(projectReport.getInputPresenceReport()));
    }


    private void prepareTenant() {
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

        // create upload in source
        UploadRequest uploadRequest = generateUpload(projectDetails, source);
        UploadDetails uploadDetails = uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), uploadRequest);
        // update status
        uploadProxy.updateUploadStatus(mainCustomerSpace, uploadDetails.getUploadId(), Upload.Status.FINISHED, null);

        // create another source in project
        SourceRequest sourceRequest1 = new SourceRequest();
        sourceRequest1.setDisplayName("ReportEnd2EndSource1");
        sourceRequest1.setProjectId(projectDetails.getProjectId());
        sourceRequest1.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        source1 = sourceProxy.createSource(mainCustomerSpace, sourceRequest1);
        UploadRequest uploadRequest1 = generateUpload(projectDetails, source1);
        UploadDetails uploadDetails1 = uploadProxy.createUpload(mainCustomerSpace, source1.getSourceId(),
                uploadRequest1);
        uploadProxy.updateUploadStatus(mainCustomerSpace, uploadDetails1.getUploadId(), Upload.Status.FINISHED, null);

        // register report in upload level
        report = DataReportServiceImplTestNG.getDataReport();
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails.getUploadId(),
                report);
        report1 = DataReportServiceImplTestNG.getDataReport();
        reportProxy.updateDataReport(mainCustomerSpace, DataReportRecord.Level.Upload, uploadDetails1.getUploadId(),
                report1);
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
}
