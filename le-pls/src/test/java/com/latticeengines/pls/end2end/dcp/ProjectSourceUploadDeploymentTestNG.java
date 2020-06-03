package com.latticeengines.pls.end2end.dcp;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestProjectProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestSourceProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestUploadProxy;

public class ProjectSourceUploadDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProjectSourceUploadDeploymentTestNG.class);

    private static final String PROJECT_NAME = "testProjectName";
    private static final String PROJECT_ID = "testProjectId";
    private static final String SOURCE_NAME = "testSourceName";
    private static final String SOURCE_ID = "SourceId";

    @Inject
    private TestProjectProxy testProjectProxy;

    @Inject
    private TestSourceProxy testSourceProxy;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private TestUploadProxy testUploadProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(testUploadProxy);
        attachProtectedProxy(testProjectProxy);
        attachProtectedProxy(testSourceProxy);
    }

    @Test(groups = "deployment")
    public void testFlow() {
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION, TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);

        ProjectDetails details = testProjectProxy.createProjectWithProjectId(PROJECT_NAME, PROJECT_ID, Project.ProjectType.Type1);
        Assert.assertEquals(PROJECT_NAME, details.getProjectDisplayName());
        GrantDropBoxAccessResponse response = details.getDropFolderAccess();
        Assert.assertNotNull(response);
        String bucket = response.getBucket();
        Assert.assertTrue(StringUtils.isNotBlank(bucket));

        // create the first source without specific source ID
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName(SOURCE_NAME);
        sourceRequest.setProjectId(PROJECT_ID);
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        Source source = testSourceProxy.createSource(sourceRequest);
        verifySourceAndAccess(source, response, true);


        // create another source with specific id under same project
        sourceRequest.setSourceId(SOURCE_ID);
        Source source2 = testSourceProxy.createSource(sourceRequest);
        verifySourceAndAccess(source2, response, true);

        // Copy test file to drop folder, then trigger dcp workflow
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        // pause source for s3 import
        testSourceProxy.pauseSourceById(source.getSourceId());
        String dropPath = UploadS3PathBuilderUtils.getDropRoot(details.getProjectId(), source2.getSourceId());
        dropPath = UploadS3PathBuilderUtils.combinePath(false, true,
                UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox()), dropPath);
        String s3FileKey = dropPath + TEST_ACCOUNT_DATA_FILE;
        testArtifactService.copyTestArtifactFile(TEST_DATA_DIR, TEST_DATA_VERSION,
                TEST_ACCOUNT_DATA_FILE, s3Bucket, s3FileKey);

        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(details.getProjectId());
        request.setSourceId(source.getSourceId());
        request.setS3FileKey(s3FileKey);
        UploadDetails uploadDetail = testUploadProxy.startImport( request);
        JobStatus completedStatus = waitForWorkflowStatus(uploadDetail.getUploadStatus().getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
        List<UploadDetails> uploadDetails = testUploadProxy.getAllBySourceId(source.getSourceId(), null);
        Assert.assertNotNull(uploadDetails);
        Assert.assertEquals(uploadDetails.size(), 1);
        uploadDetail = uploadDetails.get(0);
        UploadDetails retrievedDetail = testUploadProxy.getUpload(uploadDetail.getUploadId());
        Assert.assertEquals(JsonUtils.serialize(uploadDetail), JsonUtils.serialize(retrievedDetail));
        String token = testUploadProxy.getToken(retrievedDetail.getUploadId());
        Assert.assertNotNull(token);
    }

    @Test(groups = "deployment", dependsOnMethods = "testFlow")
    public void testGetAndDelete() {
        List<ProjectSummary> projects = testProjectProxy.getAllProjects();
        Assert.assertNotNull(projects);
        Assert.assertEquals(projects.size(), 1);
        ProjectDetails details = testProjectProxy.getProjectByProjectId(PROJECT_ID);
        Assert.assertNotNull(details);
        Assert.assertFalse(details.getDeleted());
        Assert.assertEquals(details.getProjectId(), PROJECT_ID);
        Assert.assertEquals(details.getProjectDisplayName(), PROJECT_NAME);

        GrantDropBoxAccessResponse response = details.getDropFolderAccess();
        List<Source> sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 2);
        List<Source> sources2 = testSourceProxy.getSourcesByProject(PROJECT_ID);
        Assert.assertEquals(sources2.size(), 2);
        sources.forEach(s -> verifySourceAndAccess(s, response,false));

        // delete one source
        testSourceProxy.deleteSourceById(SOURCE_ID);
        details = testProjectProxy.getProjectByProjectId(PROJECT_ID);
        Assert.assertNotNull(details);
        sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 1);
        sources2 = testSourceProxy.getSourcesByProject(PROJECT_ID);
        Assert.assertNotNull(sources2);
        Assert.assertEquals(sources2.size(), 1);
        Source source = sources.get(0);

        // soft delete project
        testProjectProxy.deleteProject(PROJECT_ID);
        SleepUtils.sleep(1000);
        projects = testProjectProxy.getAllProjects();
      
        Assert.assertFalse(CollectionUtils.isEmpty(projects));
        details = testProjectProxy.getProjectByProjectId(PROJECT_ID);
        Assert.assertNotNull(details);
        log.info("retrieved details : " + JsonUtils.serialize(details));
        Assert.assertTrue(details.getDeleted());

        // check source
        sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 1);
        sources2 = testSourceProxy.getSourcesByProject(PROJECT_ID);
        Assert.assertNotNull(sources2);
        Assert.assertEquals(sources2.size(), 1);
        source = testSourceProxy.getSource(source.getSourceId());
        Assert.assertNotNull(source);

        // delete source
        testSourceProxy.deleteSourceById(source.getSourceId());
        Assert.assertTrue(CollectionUtils.isEmpty(testSourceProxy.getSourcesByProject(PROJECT_ID)));
    }

    /**
     * check the path from source and upload file to S3 to verify access
     */
    private void verifySourceAndAccess(Source source, GrantDropBoxAccessResponse response, boolean upload) {
        Assert.assertNotNull(source);
        String bucket = response.getBucket();
        String fullPath = source.getDropFullPath();
        Assert.assertTrue(StringUtils.isNotBlank(source.getRelativePath()));
        Assert.assertTrue(StringUtils.isNotBlank(fullPath));
        String object = fullPath.substring(fullPath.indexOf(bucket) + StringUtils.length(bucket) + 1);
        object = object.substring(0, object.length() - 5);
        String prefix = object + "drop/";
        Assert.assertTrue(s3Service.objectExist(bucket, object));
        Assert.assertTrue(s3Service.objectExist(bucket, prefix));
        Assert.assertTrue(s3Service.objectExist(bucket, object + "Uploads/"));

        BasicAWSCredentialsProvider creds = //
                new BasicAWSCredentialsProvider(response.getAccessKey(), response.getSecretKey());


        AmazonS3 s3Client = AmazonS3ClientBuilder.standard() //
                .withCredentials(creds).withRegion(response.getRegion()).build();
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AmazonS3Exception.class), null);
        retry.execute(context -> {
            int count = context.getRetryCount();
            if (count > 3) {
                log.info("Verify access, attempt=" + count);
            }
            String objectKey = prefix  + "test";
            if (upload) {
                uploadFile(s3Client, bucket, objectKey);
            }
            Assert.assertTrue(s3Client.doesObjectExist(bucket, objectKey));
            List<FileProperty> result = dropBoxProxy.getFileListForPath(customerSpace, prefix, null);
            Assert.assertTrue(result.size() > 0);
            return true;
        });

    }

    private void uploadFile(AmazonS3 s3Client, String bucket, String objectKey) {
        String content = "this is one test";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());
        ObjectMetadata om = new ObjectMetadata();
        om.setSSEAlgorithm("AES256");
        PutObjectRequest request = new PutObjectRequest(bucket, objectKey, inputStream, om)
                .withCannedAcl(CannedAccessControlList.BucketOwnerRead);
        s3Client.putObject(request);
    }
}
