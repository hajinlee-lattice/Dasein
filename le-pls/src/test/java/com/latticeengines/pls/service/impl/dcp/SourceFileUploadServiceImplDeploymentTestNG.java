package com.latticeengines.pls.service.impl.dcp;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.datanucleus.util.StringUtils;
import org.joda.time.DateTime;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.pls.service.dcp.ProjectService;
import com.latticeengines.pls.service.dcp.SourceFileUploadService;
import com.latticeengines.pls.service.dcp.SourceService;
import com.latticeengines.pls.service.dcp.UploadService;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class SourceFileUploadServiceImplDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private SourceFileUploadService sourceFileUploadService;

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;

    @Inject
    private UploadService uploadService;

    @Inject
    protected WorkflowProxy workflowProxy;

    @Inject
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testUploadFile() throws IOException {
        MultipartFile multipartFile = new MockMultipartFile("TestFileName.csv",
                "Test Content = Hello World!".getBytes());
        SourceFileInfo sourceFileInfo = sourceFileUploadService.uploadFile("file_" + DateTime.now().getMillis() +
                ".csv", "TestFileName.csv", false, null, multipartFile);

        Assert.assertNotNull(sourceFileInfo);
        Assert.assertFalse(StringUtils.isEmpty(sourceFileInfo.getName()));
        Assert.assertEquals(sourceFileInfo.getDisplayName(), "TestFileName.txt");

        SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, sourceFileInfo.getName());
        Assert.assertNotNull(sourceFile);
        Assert.assertFalse(StringUtils.isEmpty(sourceFile.getPath()));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, sourceFile.getPath()));
        Assert.assertEquals(HdfsUtils.getHdfsFileContents(yarnConfiguration, sourceFile.getPath()),
                "Test Content = Hello World!");
    }

    @Test(groups = "deployment")
    public void testSubmitImport() throws IOException {
        // Create Project & Source
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName("ImportEnd2EndProject");
        projectRequest.setProjectType(Project.ProjectType.Type1);
        ProjectDetails projectDetails = projectService.createProject(customerSpace,  projectRequest, "dcp_deployment@dnb.com");
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName("ImportEnd2EndSource");
        sourceRequest.setProjectId(projectDetails.getProjectId());
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        Source source = sourceService.createSource(sourceRequest);

        InputStream dataStream = testArtifactService.readTestArtifactAsStream(TEST_DATA_DIR, TEST_DATA_VERSION, TEST_ACCOUNT_DATA_FILE);
        MultipartFile multipartFile = new MockMultipartFile("TestFileName.csv", dataStream);
        SourceFileInfo sourceFileInfo = sourceFileUploadService.uploadFile("file_" + DateTime.now().getMillis() +
                ".csv", "TestFileName.csv", false, null, multipartFile);

        ApplicationId applicationId = sourceFileUploadService.submitSourceImport(projectDetails.getProjectId(),
                source.getSourceId(), sourceFileInfo.getName());

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        List<UploadDetails> uploadDetailsList = uploadService.getAllBySourceId(source.getSourceId(),
                Upload.Status.FINISHED);

        Assert.assertTrue(CollectionUtils.isNotEmpty(uploadDetailsList));
    }
}
