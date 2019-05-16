package com.latticeengines.aws.s3.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.conditions.StringCondition;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.util.Md5Utils;
import com.latticeengines.aws.s3.S3Service;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class S3ServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static String DROP_FOLDER = "dropfolder";

    private static final String SOURCE_S3_BUCKET = "latticeengines-test-artifacts";

    @Inject
    private S3Service s3Service;

    @Inject
    private AmazonS3 s3Client;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.test.s3.bucket}")
    private String testBucket;

    @Value("${aws.customer.account.id}")
    private String accountId;

    @Value("${aws.test.customer.access.key}")
    private String customerAccessKey;

    @Value("${aws.test.customer.secret.key.encrypted}")
    private String customerSecret;

    private String dropBoxId;
    private String dropBoxDir;

    private AmazonS3 customerS3;

    @BeforeClass(groups = "functional")
    public void setup() {
        dropBoxId = RandomStringUtils.randomAlphanumeric(6).toLowerCase();
        dropBoxDir = DROP_FOLDER + "/" + dropBoxId;
        if (!s3Service.objectExist(testBucket, DROP_FOLDER)) {
            s3Service.createFolder(testBucket, DROP_FOLDER);
        }
        BasicAWSCredentialsProvider credentialsProvider = //
                new BasicAWSCredentialsProvider(customerAccessKey, customerSecret);
        customerS3 = AmazonS3ClientBuilder.standard() //
                .withCredentials(credentialsProvider).withRegion(awsRegion).build();
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        s3Service.cleanupPrefix(testBucket, dropBoxDir);
        s3Client.deleteBucketPolicy(testBucket);
    }

    @Test(groups = "functional", enabled = false)
    public void testUploadMultiPart() {
        long MB = 1024 * 1024;

        String destKey1 = "uploadMultipartStreamTest/1M.txt";
        String destKey2 = "uploadMultipartStreamTest/20M.txt";

        char[] allA = new char[(int)(MB)];
        Arrays.fill(allA, 'a');
        String oneM = new String(allA);
        InputStream oneMStream = new ByteArrayInputStream(oneM.getBytes());
        s3Service.uploadInputStreamMultiPart(testBucket, destKey1, oneMStream, MB);

        char[] allB = new char[(int)(20 * MB)];
        for (int i = 0; i < 20; i++) {
            char x = (char)('a' + i);
            Arrays.fill(allB, (int)(i * MB), (int)((i + 1) * MB - 1), x);
        }
        String twentyM = new String(allB);
        InputStream twentyMStream = new ByteArrayInputStream(twentyM.getBytes());
        s3Service.uploadInputStreamMultiPart(testBucket, destKey2, twentyMStream, 20 * MB);
    }

    @Test(groups = "functional")
    public void testCopyLargeObjectAndTag() throws IOException {
        String sourceKey = "le-serviceapps/cdl/end2end/large_csv/1/Accounts.csv";
        String destKey = "copyLargeObjectTest/Accounts.csv";
        if (!s3Service.objectExist(SOURCE_S3_BUCKET, sourceKey)) {
            //skip test if file is not there.
            return;
        }
        if (s3Service.objectExist(testBucket, destKey)) {
            s3Service.cleanupPrefix(testBucket, destKey);
            Assert.assertFalse(s3Service.objectExist(testBucket, destKey));
        }
        s3Service.copyLargeObjects(SOURCE_S3_BUCKET, sourceKey, testBucket, destKey);
        Assert.assertTrue(s3Service.objectExist(testBucket, destKey));
        //Add Tag
        s3Service.addTagToObject(testBucket, destKey, "TestTag1", "This is tag 1");
        List<Tag> tagList = s3Service.getObjectTags(testBucket, destKey);
        Assert.assertNotNull(tagList);
        Assert.assertEquals(tagList.size(), 1);
        Assert.assertEquals(tagList.get(0).getKey(), "TestTag1");
        Assert.assertEquals(tagList.get(0).getValue(), "This is tag 1");
        s3Service.addTagToObject(testBucket, destKey, "TestTag1", "This is tag 2");
        tagList = s3Service.getObjectTags(testBucket, destKey);
        Assert.assertEquals(tagList.size(), 1);
        Assert.assertEquals(tagList.get(0).getKey(), "TestTag1");
        Assert.assertEquals(tagList.get(0).getValue(), "This is tag 2");
        s3Service.addTagToObject(testBucket, destKey, "TestTag2", "This is tag 2");
        tagList = s3Service.getObjectTags(testBucket, destKey);
        Assert.assertEquals(tagList.size(), 2);

        ObjectMetadata sourceMeta = s3Client.getObjectMetadata(SOURCE_S3_BUCKET, sourceKey);
        ObjectMetadata destMeta = s3Client.getObjectMetadata(testBucket, destKey);
        Assert.assertEquals(sourceMeta.getContentLength(), destMeta.getContentLength());
        try (InputStream destSteam = s3Service.readObjectAsStream(testBucket, destKey)) {
            String destMd5 = Md5Utils.md5AsBase64(destSteam);
            try (InputStream sourceSteam = s3Service.readObjectAsStream(SOURCE_S3_BUCKET, sourceKey)) {
                String sourceMd5 = Md5Utils.md5AsBase64(sourceSteam);
                Assert.assertEquals(destMd5, sourceMd5);
            }
        }
    }

    @Test(groups = "manual", enabled = false)
    public void testChangeKey() {
        String tgtKey = "0e9daa04-1400-4e55-88c5-b238a9d01721";
        s3Service.changeKeyRecursive("latticeengines-test-dev",
                "test_kms/distcp",
                "test_kms/distcp_encrypted",
                tgtKey);
        s3Service.changeKeyRecursive("latticeengines-test-dev",
                "test_kms/distcp_encrypted",
                "test_kms/distcp_encrypted_2",
                tgtKey);
        s3Service.changeKeyRecursive("latticeengines-test-dev",
                "test_kms/distcp_encrypted",
                "test_kms/distcp_unencrypted",
                "");
    }

    @Test(groups = "functional")
    public void testBucketPolicy() {
        if (!s3Service.objectExist(testBucket, dropBoxDir)) {
            s3Service.createFolder(testBucket, dropBoxDir);
        }

        verifyNoAccess();
        Policy policy = getCustomerPolicy(dropBoxId, accountId);
        s3Service.setBucketPolicy(testBucket, policy.toJson());
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            // ignore
        }
        verifyAccess();

        String bucketPolicy = s3Service.getBucketPolicy(testBucket);
        Assert.assertTrue(bucketPolicy.contains(dropBoxId));
        policy = Policy.fromJson(bucketPolicy);
        revokeAccountFromDropBox(policy, dropBoxId, accountId);
        Assert.assertTrue(CollectionUtils.isEmpty(policy.getStatements()));

        String dropBoxId2 = RandomStringUtils.randomAlphanumeric(6).toLowerCase();
        policy = getCustomerPolicy(dropBoxId2, accountId);
        s3Service.setBucketPolicy(testBucket, policy.toJson());
        bucketPolicy = s3Service.getBucketPolicy(testBucket);
        Assert.assertTrue(bucketPolicy.contains(dropBoxId2));

        policy = Policy.fromJson(bucketPolicy);
        revokeDropBoxFromDropBox(policy, dropBoxId);
        Assert.assertFalse(policy.toJson().contains(dropBoxId), policy.toJson());
        Assert.assertTrue(policy.toJson().contains(dropBoxId2), policy.toJson());
        s3Service.setBucketPolicy(testBucket, policy.toJson());

        bucketPolicy = s3Service.getBucketPolicy(testBucket);
        Assert.assertFalse(bucketPolicy.contains(dropBoxId), bucketPolicy);
        Assert.assertTrue(bucketPolicy.contains(dropBoxId2), bucketPolicy);

        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            // ignore
        }
        verifyNoAccess();
    }

    private Policy getCustomerPolicy(String dropBoxId, String accountId) {
        String bucketPolicy = s3Service.getBucketPolicy(testBucket);
        List<Statement> statements = new ArrayList<>();
        Policy policy;
        if (StringUtils.isBlank(bucketPolicy)) {
            policy = new Policy();
        } else {
            policy = Policy.fromJson(bucketPolicy);
            revokeAccountFromDropBox(policy, dropBoxId, accountId);
        }
        boolean hasAccountStmt = false;
        if (CollectionUtils.isNotEmpty(policy.getStatements())) {
            for (Statement stmt: policy.getStatements()) {
                if (stmt.getId().equals(accountId)) {
                    insertAccountStatement(testBucket, dropBoxId, stmt);
                    hasAccountStmt = true;
                }
                statements.add(stmt);
            }
        }
        if (!hasAccountStmt) {
            statements.add(getAccountStatement(testBucket, dropBoxId, accountId));
        }
        statements.add(getAccountListDropBoxStatement(testBucket, dropBoxId, accountId));
        policy.setStatements(statements);
        return policy;
    }

    private Statement getAccountStatement(String bucketName, String dropBoxId, String accountId) {
        String arn = "arn:aws:s3:::" + bucketName + "/" + DROP_FOLDER + "/" + dropBoxId;
        return new Statement(Statement.Effect.Allow) //
                        .withId(accountId) //
                        .withPrincipals(new Principal(accountId)) //
                        .withActions(//
                                S3Actions.AbortMultipartUpload, //
                                S3Actions.GetObject, //
                                S3Actions.PutObject, //
                                S3Actions.DeleteObject, //
                                S3Actions.SetObjectAcl
                        ) //
                        .withResources(new Resource(arn + "*"));
    }

    private Statement getAccountListDropBoxStatement(String bucketName, String dropBoxId, String accountId) {
        return new Statement(Statement.Effect.Allow) //
                .withId(accountId + "_" + dropBoxId) //
                .withPrincipals(new Principal(accountId)) //
                .withActions(S3Actions.ListObjects) //
                .withResources(new Resource("arn:aws:s3:::" + bucketName))
                .withConditions(new StringCondition(//
                        StringCondition.StringComparisonType.StringLike, //
                        "s3:prefix", //
                        DROP_FOLDER + "/" + dropBoxId + "*" //
                ));
    }

    private void revokeAccountFromDropBox(Policy policy, String dropBoxId, String accountId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    if (accountId.equals(stmt.getId())) {
                        List<Resource> resourceList = stmt.getResources().stream() //
                                .filter(rsc -> !rsc.getId().contains(dropBoxId))//
                                .collect(Collectors.toList());
                        stmt.setResources(resourceList);
                    }
                }) //
                .filter(stmt -> {
                    boolean keep = true;
                    if (CollectionUtils.isEmpty(stmt.getResources())) {
                        keep = false;
                    } else if (stmt.getId().contains(accountId) && stmt.getId().contains(dropBoxId)) {
                        keep = false;
                    }
                    return keep;
                }) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private void revokeDropBoxFromDropBox(Policy policy, String dropBoxId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    List<Resource> resourceList = stmt.getResources().stream() //
                            .filter(rsc -> !rsc.getId().contains(dropBoxId))//
                            .collect(Collectors.toList());
                    stmt.setResources(resourceList);
                }) //
                .filter(stmt -> {
                    boolean keep = true;
                    if (CollectionUtils.isEmpty(stmt.getResources())) {
                        keep = false;
                    } else if (stmt.getId().contains(dropBoxId)) {
                        keep = false;
                    }
                    return keep;
                }) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private void insertAccountStatement(String bucketName, String dropBoxId, Statement statement) {
        String arn = "arn:aws:s3:::" + bucketName + "/" + DROP_FOLDER + "/" + dropBoxId;
        List<Resource> rscs = new ArrayList<>(statement.getResources());
        rscs.add(new Resource(arn));
        rscs.add(new Resource(arn + "*"));
        statement.setResources(rscs);
    }

    private void verifyAccess() {
        customerS3.doesObjectExist(testBucket, dropBoxDir);
        customerS3.listObjectsV2(testBucket, dropBoxDir);
    }

    private void verifyNoAccess() {
        try {
            customerS3.getObject(testBucket, dropBoxDir);
            Assert.fail("Should throw AmazonS3Exception.");
        } catch (AmazonS3Exception e) {
            Assert.assertTrue(e.getMessage().contains("403"), e.getMessage());
        }
        try {
            customerS3.listObjectsV2(testBucket, dropBoxDir);
            Assert.fail("Should throw AmazonS3Exception.");
        } catch (AmazonS3Exception e) {
            Assert.assertTrue(e.getMessage().contains("403"), e.getMessage());
        }
    }

}
