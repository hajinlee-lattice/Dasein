package com.latticeengines.aws.s3.impl;


import javax.inject.Inject;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class S3ServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private S3Service s3Service;

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

}
