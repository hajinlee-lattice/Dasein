package com.latticeengines.aws.s3.impl;

import java.util.ArrayList;
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
import com.latticeengines.aws.s3.S3Service;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class S3ServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static String DROP_BOX = "dropbox";

    @Inject
    private S3Service s3Service;

    @Inject
    private AmazonS3 s3Client;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.test.s3.bucket}")
    private String testBucket;

    @Value("${aws.test.customer.account.id}")
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
        dropBoxDir = DROP_BOX + "/" + dropBoxId;
        if (!s3Service.objectExist(testBucket, DROP_BOX)) {
            s3Service.createFolder(testBucket, DROP_BOX);
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
        String arn = "arn:aws:s3:::" + bucketName + "/dropbox/" + dropBoxId;
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
                        DROP_BOX + "/" + dropBoxId + "*" //
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
        String arn = "arn:aws:s3:::" + bucketName + "/dropbox/" + dropBoxId;
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
