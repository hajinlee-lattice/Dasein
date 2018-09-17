package com.latticeengines.apps.cdl.service.impl;

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
import com.latticeengines.domain.exposed.cdl.RevokeDropBoxAccessRequest;

public class DropBoxServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DropBoxService dropboxService;

    @Inject
    private S3Service s3Service;

    @Inject
    private IAMService iamService;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    @Value("${aws.region}")
    private String awsRegion;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        dropboxService.delete();
    }

    @Test(groups = "functional")
    public void testCrud() {
        String prefix = testCreate();
        testGrantAccess();
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

    private void testGrantAccess() {
        testGrantAccessToNewUser();
        testGrantAccessToExistingUser();
    }

    private void testGrantAccessToNewUser() {
        GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
        request.setAccessMode(LatticeUser);
        GrantDropBoxAccessResponse response = dropboxService.grantAccess(request);
        Assert.assertEquals(response.getAccessMode(), LatticeUser);

        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            // ignore
        }
        BasicAWSCredentialsProvider credentialsProvider = //
                new BasicAWSCredentialsProvider(response.getAccessKey(), response.getSecretKey());
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard() //
                .withCredentials(credentialsProvider).withRegion(awsRegion).build();
        verifyAccess(s3Client, true);

        String userName = response.getLatticeUser();
        RevokeDropBoxAccessRequest revokeRequest = new RevokeDropBoxAccessRequest();
        revokeRequest.setAccessMode(LatticeUser);
        revokeRequest.setLatticeUser(userName);
        dropboxService.revokeAccess(revokeRequest);
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            // ignore
        }
        verifyNoAccess(s3Client);
    }

    private void testGrantAccessToExistingUser() {
        String dropBoxId = RandomStringUtils.randomAlphanumeric(6).toLowerCase();
        String userName = "c-" + dropBoxId;
        iamService.createCustomerUser(userName);
        AccessKey accessKey = iamService.createCustomerKey(userName);
        BasicAWSCredentialsProvider credentialsProvider = //
                new BasicAWSCredentialsProvider(accessKey.getAccessKeyId(), accessKey.getSecretAccessKey());
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard() //
                .withCredentials(credentialsProvider).withRegion(awsRegion).build();
        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            // ignore
        }

        GrantDropBoxAccessRequest request = new GrantDropBoxAccessRequest();
        request.setAccessMode(LatticeUser);
        request.setExistingUser(userName);
        GrantDropBoxAccessResponse response = dropboxService.grantAccess(request);
        Assert.assertEquals(response.getAccessMode(), LatticeUser);
        Assert.assertEquals(response.getLatticeUser(), userName);
        Assert.assertNull(response.getAccessKey());

        try {
            Thread.sleep(7000L);
        } catch (InterruptedException e) {
            // ignore
        }
        verifyAccess(s3Client, false);
        RevokeDropBoxAccessRequest revokeRequest = new RevokeDropBoxAccessRequest();
        revokeRequest.setAccessMode(LatticeUser);
        revokeRequest.setLatticeUser(userName);
        dropboxService.revokeAccess(revokeRequest);
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            // ignore
        }
        verifyNoAccess(s3Client);

        iamService.deleteCustomerUser(userName);
    }

    private void verifyAccess(AmazonS3 s3Client, boolean upload) {
        String bucket = dropboxService.getDropBoxBucket();
        String prefix = dropboxService.getDropBoxPrefix();

        String objectKey = prefix + "/le.html";
        if (upload) {
            uploadFile(s3Client, bucket, objectKey);
        }
        Assert.assertTrue(s3Client.doesObjectExist(bucket, objectKey));
        ListObjectsV2Result result = s3Client.listObjectsV2(bucket, prefix);
        Assert.assertTrue(result.getKeyCount() > 0);
    }

    private void verifyNoAccess(AmazonS3 s3Client) {
        String bucket = dropboxService.getDropBoxBucket();
        String prefix = dropboxService.getDropBoxPrefix();
        String objectKey = prefix + "/le.html";
        try {
            s3Client.doesObjectExist(bucket, objectKey);
            Assert.fail("Should trow AmazonS3Exception.");
        } catch (AmazonS3Exception e) {
            Assert.assertTrue(e.getMessage().contains("Forbidden"));
        }
    }

    private void uploadFile(AmazonS3 s3Client, String bucket, String key) {
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

}
