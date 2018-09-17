package com.latticeengines.aws.iam.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.latticeengines.aws.iam.IAMService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class IAMServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private IAMService iamService;

    private String dropboxId;
    private String userName;
    private String arnPrefix;
    private String s3Bucket = "latticeengines-qa-customers";

    @BeforeClass(groups = "functional")
    public void setup() {
        dropboxId = RandomStringUtils.randomAlphanumeric(6).toLowerCase();
        userName = toUserName(dropboxId);
        arnPrefix = "arn:aws:s3:::" + s3Bucket + "/dropbox/" + dropboxId;
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

//        IAMServiceImpl impl = (IAMServiceImpl) iamService;
//        AmazonIdentityManagement iam = impl.getIamClient();
//        testUserPolicyManipulation();
    }

    private void testUserPolicyManipulation() {
        String policyName = "dropbox";
        String policyDoc = createDropBoxPolicy(policyName);
        String dropboxId2 = RandomStringUtils.randomAlphanumeric(6).toLowerCase();

        Policy policy = Policy.fromJson(policyDoc);
        policyDoc = appendDropbox(policyName, policy, dropboxId2);

        policy = Policy.fromJson(policyDoc);
        removeDropbox(policy, dropboxId2);
        Assert.assertTrue(CollectionUtils.isNotEmpty(policy.getStatements()));
        iamService.putUserPolicy(userName, policyName, policy.toJson());
        policyDoc = iamService.getUserPolicy(userName, policyName);
        Assert.assertTrue(StringUtils.isNotBlank(policyDoc));
        Assert.assertFalse(policyDoc.contains(dropboxId2));

        policy = Policy.fromJson(policyDoc);
        removeDropbox(policy, dropboxId);
        Assert.assertTrue(CollectionUtils.isEmpty(policy.getStatements()));
        iamService.deleteUserPolicy(userName, policyName);
    }

    private String createDropBoxPolicy(String policyName) {
        String policyDoc = iamService.getUserPolicy(userName, policyName);
        Assert.assertTrue(StringUtils.isBlank(policyDoc));
        Policy policy = new Policy().withStatements(//
                new Statement(Statement.Effect.Allow) //
                        .withId("ListBucket") //
                        .withActions(S3Actions.ListObjects) //
                        .withResources(//
                                new Resource(arnPrefix), //
                                new Resource(arnPrefix +  "/*") //
                        ), //
                new Statement(Statement.Effect.Allow) //
                        .withId("Objects") //
                        .withActions(//
                                S3Actions.AbortMultipartUpload, //
                                S3Actions.GetObject, //
                                S3Actions.PutObject, //
                                S3Actions.DeleteObject //
                        ) //
                        .withResources(//
                                new Resource(arnPrefix + "/*") //
                        ) //
        );
        iamService.putUserPolicy(userName, policyName, policy.toJson());
        policyDoc = iamService.getUserPolicy(userName, policyName);
        Assert.assertTrue(StringUtils.isNotBlank(policyDoc));
        return policyDoc;
    }

    private String appendDropbox(String policyName, Policy policy, String dropboxId2) {
        policy.getStatements().forEach(stmt -> {
            if ("ListBucket".equals(stmt.getId())) {
                List<Resource> resourceList = stmt.getResources();
                Set<String> resources = resourceList.stream().map(Resource::getId)
                        .collect(Collectors.toSet());
                for (String rsc : Arrays.asList(
                        "arn:aws:s3:::" + s3Bucket + "/dropbox/" + dropboxId2,
                        "arn:aws:s3:::" + s3Bucket + "/dropbox/" + dropboxId2 + "/*")) {
                    if (!resources.contains(rsc)) {
                        resourceList.add(new Resource(rsc));
                        resources.add(rsc);
                    }
                }
            } else if ("Objects".equals(stmt.getId())) {
                List<Resource> resourceList = stmt.getResources();
                Set<String> resources = resourceList.stream().map(Resource::getId)
                        .collect(Collectors.toSet());
                String rsc = "arn:aws:s3:::" + s3Bucket + "/dropbox/" + dropboxId2 + "/*";
                if (!resources.contains(rsc)) {
                    resourceList.add(new Resource(rsc));
                    resources.add(rsc);
                }
            }
        });
        iamService.putUserPolicy(userName, policyName, policy.toJson());
        String policyDoc = iamService.getUserPolicy(userName, policyName);
        Assert.assertTrue(StringUtils.isNotBlank(policyDoc));
        Assert.assertTrue(policyDoc.contains(dropboxId2));
        return policyDoc;
    }

    private void removeDropbox(Policy policy, String dropboxId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    List<Resource> resourceList = stmt.getResources().stream() //
                            .filter(rsc -> !rsc.getId().contains(dropboxId))//
                            .collect(Collectors.toList());
                    stmt.setResources(resourceList);
                }) //
                .filter(stmt -> CollectionUtils.isNotEmpty(stmt.getResources())) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private String toUserName(String dropboxId) {
        return "c-test-" + dropboxId;
    }

}
