package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.DropBoxAccessMode.LatticeUser;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.identitymanagement.model.AccessKey;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.aws.iam.IAMService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.security.Tenant;

public class DropBoxServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DropBoxServiceImplTestNG.class);

    @Inject
    private DropBoxService dropboxService;

    @Inject
    private S3Service s3Service;

    @Inject
    private IAMService iamService;

    @Value("${aws.test.s3.bucket}")
    private String testBucket;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.customer.account.id}")
    private String accountId;

    @Value("${aws.test.customer.access.key}")
    private String customerAccessKey;

    @Value("${aws.test.customer.secret.key.encrypted}")
    private String customerSecret;

    @Resource(name = "awsCredentials")
    private BasicAWSCredentials awsCredentials;

    private BasicAWSCredentialsProvider latticeProvider;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        latticeProvider = new BasicAWSCredentialsProvider(awsCredentials.getAWSAccessKeyId(), //
                awsCredentials.getAWSSecretKey());
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        dropboxService.delete();
    }

    @Test(groups = "functional")
    public void testCrudLatticeUser() {
        String prefix = testCreate();
        testGrantAccessToNewUser();
        testGrantAccessToExistingUser();
        testDelete(prefix);
    }

    private String testCreate() {
        dropboxService.create();
        String prefix = dropboxService.getDropBoxPrefix();
        Assert.assertTrue(StringUtils.isNotBlank(prefix));
        Assert.assertTrue(s3Service.objectExist(dropboxService.getDropBoxBucket(), prefix + "/"));
        Tenant owner = dropboxService.getDropBoxOwner(dropboxService.getDropBoxSummary().getDropBox());
        Assert.assertNotNull(owner);
        Assert.assertEquals(owner.getPid(), mainTestTenant.getPid());
        return prefix;
    }

    private void testDelete(String prefix) {
        dropboxService.delete();
        Assert.assertFalse(StringUtils.isNotBlank(dropboxService.getDropBoxPrefix()));
        Assert.assertFalse(s3Service.objectExist(dropboxService.getDropBoxBucket(), prefix + "/"));
    }

    private void testGrantAccessToNewUser() {
        GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
        request.setAccessMode(LatticeUser);
        GrantDropBoxAccessResponse response = dropboxService.grantAccess(request);
        Assert.assertNotNull(response.getRegion());
        Assert.assertEquals(response.getAccessMode(), LatticeUser);

        BasicAWSCredentialsProvider creds = //
                new BasicAWSCredentialsProvider(response.getAccessKey(), response.getSecretKey());
        verifyAccessWithRetries(creds, true);
        verifyAccessWithRetries(latticeProvider, false);

        DropBoxSummary summary = dropboxService.getDropBoxSummary();
        Assert.assertEquals(summary.getAccessKeyId(), response.getAccessKey());

        GrantDropBoxAccessResponse newResponse = dropboxService.refreshAccessKey();
        Assert.assertNotNull(newResponse.getBucket());
        Assert.assertNotNull(newResponse.getDropBox());
        Assert.assertNotNull(newResponse.getRegion());
        BasicAWSCredentialsProvider newCreds = //
                new BasicAWSCredentialsProvider(newResponse.getAccessKey(), newResponse.getSecretKey());
        verifyAccessWithRetries(newCreds, false);

        dropboxService.revokeAccess();
        verifyNoAccessWithRetries(creds, false);
        verifyAccessWithRetries(latticeProvider, false);
    }

    private void testGrantAccessToExistingUser() {
        String dropBoxId = RandomStringUtils.randomAlphanumeric(6).toLowerCase();
        String userName = "c-" + dropBoxId;
        iamService.createCustomerUser(userName);
        AccessKey accessKey = iamService.createCustomerKey(userName);

        GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
        request.setAccessMode(LatticeUser);
        request.setExistingUser(userName);
        GrantDropBoxAccessResponse response = dropboxService.grantAccess(request);
        Assert.assertEquals(response.getAccessMode(), LatticeUser);
        Assert.assertEquals(response.getLatticeUser(), userName);
        Assert.assertNull(response.getAccessKey());

        BasicAWSCredentialsProvider creds = //
                new BasicAWSCredentialsProvider(accessKey.getAccessKeyId(), accessKey.getSecretAccessKey());
        verifyAccessWithRetries(creds, false);
        verifyAccessWithRetries(latticeProvider, false);

        DropBoxSummary summary = dropboxService.getDropBoxSummary();
        Assert.assertEquals(summary.getAccessKeyId(), accessKey.getAccessKeyId());

        dropboxService.revokeAccess();
        verifyNoAccessWithRetries(creds, false);
        verifyAccessWithRetries(latticeProvider, false);

        iamService.deleteCustomerUser(userName);
    }

    private void verifyAccessWithRetries(BasicAWSCredentialsProvider creds, boolean upload) {
        String bucket = dropboxService.getDropBoxBucket();
        String prefix = dropboxService.getDropBoxPrefix();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard() //
                .withCredentials(creds).withRegion(awsRegion).build();
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AmazonS3Exception.class), null);
        retry.execute(context -> {
            int count = context.getRetryCount();
            if (count > 3) {
                log.info("Verify access, attempt=" + count);
            }
            String objectKey = prefix + "/le.csv";
            if (upload) {
                uploadFile(s3Client, bucket, prefix);
            }
            Assert.assertTrue(s3Client.doesObjectExist(bucket, objectKey));
            List<FileProperty> result = dropboxService.getFileListForPath(mainCustomerSpace
            , prefix);
            log.info(JsonUtils.serialize(result));
            Assert.assertTrue(result.size() > 0);
            return true;
        });

    }

    private void verifyNoAccessWithRetries(BasicAWSCredentialsProvider creds, boolean skipGet) {
        String bucket = dropboxService.getDropBoxBucket();
        String prefix = dropboxService.getDropBoxPrefix();
        String objectKey = prefix + "/le.html";

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard() //
                .withCredentials(creds).withRegion(awsRegion).build();
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            int count = context.getRetryCount();
            if (count > 3) {
                log.info("Verify no list object access, attempt=" + count);
            }
            verifyNoAccess(s3Client, bucket, objectKey);
            return true;
        });
        if (!skipGet) {
            retry.execute(context -> {
                int count = context.getRetryCount();
                if (count > 3) {
                    log.info("Verify no get object access, attempt=" + count);
                }
                try {
                    s3Client.getObject(bucket, objectKey);
                    Assert.fail("Should throw AmazonS3Exception.");
                } catch (AmazonS3Exception e) {
                    Assert.assertTrue(e.getMessage().contains("403"));
                }
                return true;
            });
        }
    }

    private void verifyNoAccess(AmazonS3 s3Client, String bucket, String objectKey) {
        try {
            s3Client.listObjectsV2(bucket, objectKey);
            Assert.fail("Should throw AmazonS3Exception.");
        } catch (AmazonS3Exception e) {
            Assert.assertTrue(e.getMessage().contains("403"));
        }
    }

    private void uploadFile(AmazonS3 s3Client, String bucket, String prefix) {
        String key = prefix + "/le.csv";
        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("dropbox/le.html");
        ObjectMetadata om = new ObjectMetadata();
        om.setSSEAlgorithm("AES256");
        PutObjectRequest request = new PutObjectRequest(bucket, key, inputStream, om)
                .withCannedAcl(CannedAccessControlList.BucketOwnerRead);
        s3Client.putObject(request);
    }

}
