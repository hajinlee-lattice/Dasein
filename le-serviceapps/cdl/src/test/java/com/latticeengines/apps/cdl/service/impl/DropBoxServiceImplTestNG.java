package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.DropBoxAccessMode.ExternalAccount;
import static com.latticeengines.domain.exposed.cdl.DropBoxAccessMode.LatticeUser;

import java.io.InputStream;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.identitymanagement.model.AccessKey;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.aws.iam.IAMService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;

public class DropBoxServiceImplTestNG extends CDLFunctionalTestNGBase {

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

    @Value("${aws.test.customer.account.id}")
    private String accountId;

    @Value("${aws.test.customer.access.key}")
    private String customerAccessKey;

    @Value("${aws.test.customer.secret.key.encrypted}")
    private String customerSecret;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
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

    @Test(groups = "functional", dependsOnMethods = "testCrudLatticeUser")
    // @Test(groups = "functional")
    public void testCrudExternalAccount() {
        DropBoxServiceImpl impl = (DropBoxServiceImpl) dropboxService;
        impl.setCustomersBucket(testBucket);
        String prefix = testCreate();
        testGrantAccessToExternalAccount();
        testDelete(prefix);
    }

    private String testCreate() {
        dropboxService.create();
        String prefix = dropboxService.getDropBoxPrefix();
        Assert.assertTrue(StringUtils.isNotBlank(prefix));
        Assert.assertTrue(s3Service.objectExist(dropboxService.getDropBoxBucket(), prefix + "/"));
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
        Assert.assertEquals(response.getAccessMode(), LatticeUser);

        waitPolicyTakeEffect();
        BasicAWSCredentialsProvider creds = //
                new BasicAWSCredentialsProvider(response.getAccessKey(), response.getSecretKey());
        verifyAccess(creds, true);

        dropboxService.revokeAccess();
        waitPolicyTakeEffect();
        verifyNoAccess(creds, false);
    }

    private void testGrantAccessToExistingUser() {
        String dropBoxId = RandomStringUtils.randomAlphanumeric(6).toLowerCase();
        String userName = "c-" + dropBoxId;
        iamService.createCustomerUser(userName);
        AccessKey accessKey = iamService.createCustomerKey(userName);
        BasicAWSCredentialsProvider creds = //
                new BasicAWSCredentialsProvider(accessKey.getAccessKeyId(), accessKey.getSecretAccessKey());

        GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
        request.setAccessMode(LatticeUser);
        request.setExistingUser(userName);
        GrantDropBoxAccessResponse response = dropboxService.grantAccess(request);
        Assert.assertEquals(response.getAccessMode(), LatticeUser);
        Assert.assertEquals(response.getLatticeUser(), userName);
        Assert.assertNull(response.getAccessKey());

        waitPolicyTakeEffect();
        verifyAccess(creds, false);
        dropboxService.revokeAccess();
        waitPolicyTakeEffect();
        verifyNoAccess(creds, false);

        iamService.deleteCustomerUser(userName);
    }

    private void testGrantAccessToExternalAccount() {
        GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
        request.setAccessMode(ExternalAccount);
        request.setExternalAccountId(accountId);
        GrantDropBoxAccessResponse response = dropboxService.grantAccess(request);
        Assert.assertEquals(response.getAccessMode(), ExternalAccount);

        waitPolicyTakeEffect();
        BasicAWSCredentialsProvider creds = //
                new BasicAWSCredentialsProvider(customerAccessKey, customerSecret);
        verifyAccess(creds, true);

        dropboxService.revokeAccess();
        waitPolicyTakeEffect();
        verifyNoAccess(creds, true);
    }

    private void verifyAccess(BasicAWSCredentialsProvider creds, boolean upload) {
        String bucket = dropboxService.getDropBoxBucket();
        String prefix = dropboxService.getDropBoxPrefix();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard() //
                .withCredentials(creds).withRegion(awsRegion).build();
        String objectKey = prefix + "/le.html";
        if (upload) {
            uploadFile(s3Client, bucket, prefix);
        }
        Assert.assertTrue(s3Client.doesObjectExist(bucket, objectKey));
        ListObjectsV2Result result = s3Client.listObjectsV2(bucket, prefix);
        Assert.assertTrue(result.getKeyCount() > 0);
    }

    private void verifyNoAccess(BasicAWSCredentialsProvider creds, boolean skipGet) {
        String bucket = dropboxService.getDropBoxBucket();
        String prefix = dropboxService.getDropBoxPrefix();
        String objectKey = prefix + "/le.html";

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard() //
                .withCredentials(creds).withRegion(awsRegion).build();
        try {
            s3Client.listObjectsV2(bucket, objectKey);
            Assert.fail("Should throw AmazonS3Exception.");
        } catch (AmazonS3Exception e) {
            Assert.assertTrue(e.getMessage().contains("403"));
        }
        if (!skipGet) {
            try {
                s3Client.getObject(bucket, objectKey);
                Assert.fail("Should throw AmazonS3Exception.");
            } catch (AmazonS3Exception e) {
                Assert.assertTrue(e.getMessage().contains("403"));
            }
        }
    }

    private void uploadFile(AmazonS3 s3Client, String bucket, String prefix) {
        String key = prefix + "/le.html";
        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("dropbox/le.html");
        TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        Upload upload = tm.upload(bucket, key, inputStream, null);
        try {
            upload.waitForCompletion();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitPolicyTakeEffect() {
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            // ignore
        }
    }

}
