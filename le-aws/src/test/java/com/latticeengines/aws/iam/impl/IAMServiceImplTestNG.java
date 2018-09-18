package com.latticeengines.aws.iam.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.iam.IAMService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class IAMServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private IAMService iamService;

    private String userName;

    @BeforeClass(groups = "functional")
    public void setup() {
        String dropboxId = RandomStringUtils.randomAlphanumeric(6).toLowerCase();
        userName = toUserName(dropboxId);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        iamService.deleteCustomerUser(userName);
        iamService.deleteCustomerUser(userName);
    }

    @Test(groups = "functional")
    public void testUserCrud() {
        String arn = iamService.createCustomerUser(userName);
        Assert.assertNotNull(arn);
        Assert.assertTrue(arn.endsWith(String.format("user/customers/%s", userName)));

        arn = iamService.createCustomerUser(userName);
        Assert.assertNotNull(arn);
        Assert.assertTrue(arn.endsWith(String.format("user/customers/%s", userName)));
    }

    private String toUserName(String dropboxId) {
        return "c-test-" + dropboxId;
    }

}
