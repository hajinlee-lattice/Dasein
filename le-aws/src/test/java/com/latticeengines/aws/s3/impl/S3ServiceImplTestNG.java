package com.latticeengines.aws.s3.impl;


import javax.inject.Inject;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.conditions.StringCondition;
import com.amazonaws.services.s3.AmazonS3;
import com.latticeengines.aws.s3.S3Service;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class S3ServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private S3Service s3Service;

    @Inject
    private AmazonS3 s3Client;

    @Test(groups = "functional", enabled = false)
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

    @Test(groups = "functional", enabled = false)
    public void testBucketPolicy() {
        String bucket_name = "le-test-kms";
        Policy bucket_policy = new Policy().withStatements( //
                new Statement(Statement.Effect.Allow) //
                        .withPrincipals(Principal.AllUsers) //
                        .withActions(S3Actions.PutObject) //
                        .withResources(new Resource("arn:aws:s3:::" + bucket_name + "/*")) //
                        .withConditions( //
                                new StringCondition(StringCondition.StringComparisonType.StringEquals, "s3:x-amz-server-side-encryption", "AES256") //
                        ), //
                new Statement(Statement.Effect.Allow) //
                        .withPrincipals(Principal.AllUsers) //
                        .withActions(S3Actions.PutObject) //
                        .withResources(new Resource("arn:aws:s3:::" + bucket_name + "/test_ysong/*")) //
                        .withConditions( //
                                new StringCondition(StringCondition.StringComparisonType.StringEquals, "s3:x-amz-server-side-encryption", "aws:kms"), //
                                new StringCondition(StringCondition.StringComparisonType.StringEquals, "s3:x-amz-server-side-encryption-aws-kms-key-id", "test/ysong") //
                        )
        );
        String json = bucket_policy.toJson();
        System.out.println(json);
        // s3Client.setBucketPolicy(bucket_name, json);
    }

}
