package com.latticeengines.pls.end2end.dcp;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.CollectionUtils;
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
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.pls.service.dcp.ProjectService;
import com.latticeengines.pls.service.dcp.SourceService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

public class ProjectSourceUploadDeploymentTestNG extends DCPDeploymentTestNGBase {


    private static final Logger log = LoggerFactory.getLogger(ProjectSourceUploadDeploymentTestNG.class);

    private static final String PROJECT_NAME = "testProjectName";

    private static final String PROJECT_ID = "testProjectId";

    private static final String SOURCE_NAME = "testSourceName";

    private static final String SOURCE_ID = "SourceId";

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment", enabled = false)
    public void testFlow() {
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION, TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);

        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName(PROJECT_NAME);
        projectRequest.setProjectId(PROJECT_ID);
        projectRequest.setProjectType(Project.ProjectType.Type1);
        ProjectDetails details = projectService.createProject(customerSpace, projectRequest,
                MultiTenantContext.getEmailAddress());
        Assert.assertEquals(PROJECT_NAME, details.getProjectDisplayName());
        GrantDropBoxAccessResponse response = details.getDropFolderAccess();
        Assert.assertNotNull(response);
        String bucket = response.getBucket();
        Assert.assertTrue(StringUtils.isNotBlank(bucket));

        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName(SOURCE_NAME);
        sourceRequest.setProjectId(PROJECT_ID);
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        Source accountSource = sourceService.createSource(sourceRequest);
        verifySourceAndAccess(accountSource, response, true);


        // create another source with id under same project
        sourceRequest.setSourceId(SOURCE_ID);
        Source contactSource = sourceService.createSource(sourceRequest);
        verifySourceAndAccess(contactSource, response, true);
        
        // TODO(penglong) drop file to s3 to trigger flow




    }

    @Test(groups = "deployment", dependsOnMethods = "testFlow", enabled = false)
    public void testGetAndDelete() {
        List<Project> projects = projectService.getAllProjects(customerSpace);
        Assert.assertNotNull(projects);
        Assert.assertEquals(projects.size(), 1);
        ProjectDetails details = projectService.getProjectByProjectId(customerSpace, PROJECT_ID);
        Assert.assertNotNull(details);
        Assert.assertFalse(details.getDeleted());
        Assert.assertEquals(details.getProjectId(), PROJECT_ID);
        Assert.assertEquals(details.getProjectDisplayName(), PROJECT_NAME);

        GrantDropBoxAccessResponse response = details.getDropFolderAccess();
        List<Source> sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 2);
        List<Source> sources2 = sourceService.getSourceList(PROJECT_ID);
        Assert.assertEquals(sources2.size(), 2);
        sources.forEach(s -> verifySourceAndAccess(s, response,false));

        // delete one source
        sourceService.deleteSource(SOURCE_ID);
        details = projectService.getProjectByProjectId(customerSpace, PROJECT_ID);
        Assert.assertNotNull(details);
        sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 1);
        sources2 = sourceService.getSourceList(PROJECT_ID);
        Assert.assertNotNull(sources2);
        Assert.assertEquals(sources2.size(), 1);
        Source source = sources.get(0);

        // soft delete project
        projectService.deleteProject(customerSpace, PROJECT_ID);
        projects = projectService.getAllProjects(customerSpace);
        Assert.assertFalse(CollectionUtils.isEmpty(projects));
        details = projectService.getProjectByProjectId(customerSpace, PROJECT_ID);
        Assert.assertNotNull(details);
        Assert.assertTrue(details.getDeleted());

        // check source
        sources = details.getSources();
        Assert.assertNotNull(sources);
        Assert.assertEquals(sources.size(), 1);
        sources2 = sourceService.getSourceList(PROJECT_ID);
        Assert.assertNotNull(sources2);
        Assert.assertEquals(sources2.size(), 1);
        source = sourceService.getSource(source.getSourceId());
        Assert.assertNotNull(source);

        // delete source
        sourceService.deleteSource(source.getSourceId());
        Assert.assertTrue(CollectionUtils.isEmpty(sourceService.getSourceList(PROJECT_ID)));
    }

    /**
     * check the path from source and upload file to S3 to verify access
     * @param source
     * @param response
     * @param upload
     */
    private void verifySourceAndAccess(Source source, GrantDropBoxAccessResponse response, boolean upload) {
        Assert.assertNotNull(source);
        String bucket = response.getBucket();
        String fullPath = source.getFullPath();
        Assert.assertTrue(StringUtils.isNotBlank(source.getRelativePath()));
        Assert.assertTrue(StringUtils.isNotBlank(fullPath));
        String object = fullPath.substring(fullPath.indexOf(bucket) + StringUtils.length(bucket) + 1);
        String prefix = object + "drop/";
        Assert.assertTrue(s3Service.objectExist(bucket, object));
        Assert.assertTrue(s3Service.objectExist(bucket, prefix));
        Assert.assertTrue(s3Service.objectExist(bucket, object + "upload/"));

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
