package com.latticeengines.pls.controller.dcp;

import static com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration.ANALYSIS_PERCENTAGE;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadJobDetails;
import com.latticeengines.domain.exposed.dcp.UploadJobStep;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.testframework.exposed.proxy.pls.FileUploadProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestProjectProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestSourceProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestUploadProxy;

public class UploadResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";
    private static final String FILE_NAME = "file1.csv";
    private static final String PROJECT_NAME = "ImportEnd2EndProject";
    private static final String SOURCE_NAME = "ImportEnd2EndSource";

    @Inject
    private FileUploadProxy fileUploadProxy;

    @Inject
    private TestProjectProxy testProjectProxy;

    @Inject
    private TestSourceProxy testSourceProxy;

    @Inject
    private TestUploadProxy testUploadProxy;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        attachProtectedProxy(testUploadProxy);
        attachProtectedProxy(testProjectProxy);
        attachProtectedProxy(testSourceProxy);
        attachProtectedProxy(fileUploadProxy);
    }

    @Test(groups = "deployment")
    public void testUploadFile() throws IOException {
        Resource csvResource = new ClassPathResource(PATH,
                Thread.currentThread().getContextClassLoader());
        SourceFileInfo sourceFileInfo = fileUploadProxy.uploadFile(FILE_NAME, csvResource);

        Assert.assertNotNull(sourceFileInfo);
        Assert.assertFalse(StringUtils.isEmpty(sourceFileInfo.getFileImportId()));
        Assert.assertEquals(sourceFileInfo.getDisplayName(), FILE_NAME);

        SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, sourceFileInfo.getFileImportId());
        Assert.assertNotNull(sourceFile);
        Assert.assertFalse(StringUtils.isEmpty(sourceFile.getPath()));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, sourceFile.getPath()));
    }

    @Test(groups = "deployment")
    public void testSubmitImport() {
        // Create Project & Source
        ProjectDetails projectDetails = testProjectProxy.createProjectWithOutProjectId(PROJECT_NAME,
                Project.ProjectType.Type1, null);
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName(SOURCE_NAME);
        sourceRequest.setProjectId(projectDetails.getProjectId());
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        Source source = testSourceProxy.createSource(sourceRequest);

        Resource csvResource = new ClassPathResource(PATH,
                Thread.currentThread().getContextClassLoader());
        SourceFileInfo sourceFileInfo = fileUploadProxy.uploadFile(FILE_NAME, csvResource);

        DCPImportRequest dcpImportRequest = new DCPImportRequest();
        dcpImportRequest.setProjectId(projectDetails.getProjectId());
        dcpImportRequest.setSourceId(source.getSourceId());
        dcpImportRequest.setFileImportId(sourceFileInfo.getFileImportId());
        UploadDetails uploadDetails = testUploadProxy.startImport(dcpImportRequest);
        Assert.assertNotNull(uploadDetails);
        System.out.println("Before Job Complete - UploadDetails:\n" + JsonUtils.pprint(uploadDetails));
        Assert.assertNotNull(uploadDetails.getUploadDiagnostics());

        JobStatus completedStatus = waitForWorkflowStatus(uploadDetails.getUploadDiagnostics().getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);

        List<UploadDetails> uploadDetailsList = testUploadProxy.getAllBySourceId(source.getSourceId(),
                Upload.Status.FINISHED);
        Assert.assertTrue(CollectionUtils.isNotEmpty(uploadDetailsList));

        uploadDetailsList = testUploadProxy.getAllBySourceId(source.getSourceId(), null);
        Assert.assertNotNull(uploadDetailsList);
        Assert.assertEquals(uploadDetailsList.size(), 1);
        Assert.assertNotNull(uploadDetailsList.get(0));
        Assert.assertEquals(uploadDetailsList.get(0).getUploadId(), uploadDetails.getUploadId());

        System.out.println("After Job Complete - UploadDetails:\n" + JsonUtils.pprint(uploadDetailsList.get(0)));

        UploadDetails uploadDetailsCheck = testUploadProxy.getUpload(uploadDetails.getUploadId());
        Assert.assertNotNull(uploadDetailsCheck);
        Assert.assertEquals(uploadDetailsCheck.getUploadId(), uploadDetails.getUploadId());
        Assert.assertEquals(JsonUtils.serialize(uploadDetailsCheck), JsonUtils.serialize(uploadDetailsList.get(0)));

        String token = testUploadProxy.getToken(uploadDetails.getUploadId());
        Assert.assertNotNull(token);

        UploadJobDetails uploadJobDetails = testUploadProxy.getJobDetailsByUploadId(uploadDetails.getUploadId());
        Assert.assertNotNull(uploadJobDetails);
        System.out.println("After Job Complete - UploadJobDetails:\n" + JsonUtils.pprint(uploadJobDetails));

        // Validate UploadJobDetails fields.
        Assert.assertTrue(StringUtils.isNotBlank(uploadJobDetails.getDisplayName()));
        Assert.assertEquals(uploadJobDetails.getDisplayName(), FILE_NAME);

        Assert.assertTrue(StringUtils.isNotBlank(uploadJobDetails.getSourceDisplayName()));
        Assert.assertEquals(uploadJobDetails.getSourceDisplayName(), SOURCE_NAME);

        Assert.assertNotNull(uploadJobDetails.getStatus());
        Assert.assertEquals(uploadJobDetails.getStatus(), Upload.Status.FINISHED);

        Assert.assertNotNull(uploadJobDetails.getStatistics());
        Assert.assertNotNull(uploadJobDetails.getStatistics().getImportStats());
        UploadStats.ImportStats importStats = uploadJobDetails.getStatistics().getImportStats();
        Assert.assertEquals(importStats.getSubmitted().longValue(), 4L);
        Assert.assertEquals(importStats.getSuccessfullyIngested().longValue(), 4L);
        Assert.assertEquals(importStats.getFailedIngested().longValue(), 0L);

        Assert.assertNotNull(uploadJobDetails.getStatistics().getMatchStats());
        UploadStats.MatchStats matchStats = uploadJobDetails.getStatistics().getMatchStats();
        Assert.assertEquals(matchStats.getMatched().longValue(), 0L);
        Assert.assertEquals(matchStats.getUnmatched().longValue(), 4L);
        Assert.assertEquals(matchStats.getPendingReviewCnt().longValue(), 0L);

        Assert.assertNotNull(uploadJobDetails.getUploadDiagnostics());
        Assert.assertTrue(StringUtils.isNotBlank(uploadJobDetails.getUploadDiagnostics().getApplicationId()));
        Assert.assertTrue(uploadJobDetails.getUploadDiagnostics().getApplicationId().startsWith("application_"));

        Assert.assertNotNull(uploadJobDetails.getUploadJobSteps());
        Assert.assertEquals(uploadJobDetails.getUploadJobSteps().size(), 3);
        Set<String> setNameSet = uploadJobDetails.getUploadJobSteps().stream()
                .map(UploadJobStep::getStepName).collect(Collectors.toSet());
        // TODO: Find a way to avoid hard coding the step names.
        Assert.assertTrue(setNameSet.contains("Ingestion"));
        Assert.assertTrue(setNameSet.contains("Match_Append"));
        Assert.assertTrue(setNameSet.contains("Analysis"));

        Assert.assertNotNull(uploadJobDetails.getDropFileTime());
        // Assert the time greater than May 20, 2020, a valid Epoch time in milliseconds.
        Assert.assertTrue(uploadJobDetails.getDropFileTime() > 1590000000000L);

        Assert.assertNotNull(uploadJobDetails.getUploadCreatedTime());
        // Assert the time greater than May 20, 2020, a valid Epoch time in milliseconds.
        Assert.assertTrue(uploadJobDetails.getUploadCreatedTime() > 1590000000000L);

        Assert.assertNull(uploadJobDetails.getCurrentStep());
        Assert.assertNotNull(uploadJobDetails.getProgressPercentage());
        Assert.assertEquals(uploadJobDetails.getProgressPercentage(), Double.valueOf(ANALYSIS_PERCENTAGE));




        Assert.assertEquals(uploadJobDetails.getUploadJobSteps().size(), 3);
        Assert.assertNull(uploadJobDetails.getCurrentStep());



    }
}
