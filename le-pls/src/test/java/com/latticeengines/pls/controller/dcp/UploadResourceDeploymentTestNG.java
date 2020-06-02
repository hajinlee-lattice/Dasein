package com.latticeengines.pls.controller.dcp;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.FileUploadProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestProjectProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestSourceProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestUploadProxy;

public class UploadResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";
    private static final String fileName = "file1.csv";

    @Inject
    private FileUploadProxy fileUploadProxy;

    @Inject
    private TestProjectProxy testProjectProxy;

    @Inject
    private TestSourceProxy testSourceProxy;

    @Inject
    private TestUploadProxy testUploadProxy;

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
    public void testUploadFile() {
        Resource csvResource = new ClassPathResource(PATH,
                Thread.currentThread().getContextClassLoader());
        SourceFileInfo sourceFileInfo = fileUploadProxy.uploadFile(fileName, csvResource);

        Assert.assertNotNull(sourceFileInfo);
        Assert.assertFalse(StringUtils.isEmpty(sourceFileInfo.getFileImportId()));
        Assert.assertEquals(sourceFileInfo.getDisplayName(), fileName);
    }

    @Test(groups = "deployment")
    public void testSubmitImport() {
        // Create Project & Source
        ProjectDetails projectDetails = testProjectProxy.createProjectWithOutProjectId("ImportEnd2EndProject",
                Project.ProjectType.Type1);
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName("ImportEnd2EndSource");
        sourceRequest.setProjectId(projectDetails.getProjectId());
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        Source source = testSourceProxy.createSource(sourceRequest);

        Resource csvResource = new ClassPathResource(PATH,
                Thread.currentThread().getContextClassLoader());
        SourceFileInfo sourceFileInfo = fileUploadProxy.uploadFile(fileName, csvResource);

        DCPImportRequest dcpImportRequest = new DCPImportRequest();
        dcpImportRequest.setProjectId(projectDetails.getProjectId());
        dcpImportRequest.setSourceId(source.getSourceId());
        dcpImportRequest.setFileImportId(sourceFileInfo.getFileImportId());
        UploadDetails uploadDetails = testUploadProxy.startImport(dcpImportRequest);

        JobStatus completedStatus = waitForWorkflowStatus(uploadDetails.getUploadStatus().getApplicationId(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        List<UploadDetails> uploadDetailsList = testUploadProxy.getAllBySourceId(source.getSourceId(),
                Upload.Status.FINISHED);

        Assert.assertTrue(CollectionUtils.isNotEmpty(uploadDetailsList));

        Assert.assertNotNull(uploadDetails);

        List<UploadDetails> uploadDetailList = testUploadProxy.getAllBySourceId(source.getSourceId(), null);
        Assert.assertEquals(uploadDetailList.size(), 1);
        Assert.assertEquals(uploadDetailList.get(0).getUploadId(), uploadDetails.getUploadId());

        UploadDetails uploadDetailsCheck = testUploadProxy.getUpload(uploadDetails.getUploadId());
        Assert.assertNotNull(uploadDetailsCheck);
        Assert.assertEquals(uploadDetailsCheck.getUploadId(), uploadDetails.getUploadId());

        String token = testUploadProxy.getToken(uploadDetails.getUploadId());
        Assert.assertNotNull(token);
    }
}
