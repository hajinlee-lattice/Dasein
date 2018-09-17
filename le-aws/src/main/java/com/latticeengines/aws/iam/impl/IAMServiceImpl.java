package com.latticeengines.aws.iam.impl;

import java.net.URLDecoder;
import java.util.Collections;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.AccessKey;
import com.amazonaws.services.identitymanagement.model.AddUserToGroupRequest;
import com.amazonaws.services.identitymanagement.model.CreateAccessKeyRequest;
import com.amazonaws.services.identitymanagement.model.CreateAccessKeyResult;
import com.amazonaws.services.identitymanagement.model.CreateUserRequest;
import com.amazonaws.services.identitymanagement.model.CreateUserResult;
import com.amazonaws.services.identitymanagement.model.DeleteAccessKeyRequest;
import com.amazonaws.services.identitymanagement.model.DeleteUserPolicyRequest;
import com.amazonaws.services.identitymanagement.model.DeleteUserRequest;
import com.amazonaws.services.identitymanagement.model.DeleteUserResult;
import com.amazonaws.services.identitymanagement.model.GetUserPolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetUserPolicyResult;
import com.amazonaws.services.identitymanagement.model.GetUserRequest;
import com.amazonaws.services.identitymanagement.model.GetUserResult;
import com.amazonaws.services.identitymanagement.model.ListAccessKeysRequest;
import com.amazonaws.services.identitymanagement.model.ListAccessKeysResult;
import com.amazonaws.services.identitymanagement.model.ListGroupsForUserRequest;
import com.amazonaws.services.identitymanagement.model.ListGroupsForUserResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.identitymanagement.model.PutUserPolicyRequest;
import com.amazonaws.services.identitymanagement.model.RemoveUserFromGroupRequest;
import com.latticeengines.aws.iam.IAMService;
import com.latticeengines.common.exposed.util.RetryUtils;

@Service
public class IAMServiceImpl implements IAMService {

    private static final Logger log = LoggerFactory.getLogger(IAMServiceImpl.class);

    @Resource(name = "customerCredentials")
    private AWSCredentials customerCredentials;

    @Value("${aws.region}")
    private String region;

    @Value("${aws.customer.iam.group}")
    private String customerGroup;

    private AmazonIdentityManagement iamClient;

    @Override
    public String createCustomerUser(String userName) {
        String arn = getCustomerUserArn(userName);
        AmazonIdentityManagement iam = iamClient();
        if (StringUtils.isNotBlank(arn)) {
            log.info("User " + userName + " already exists with arn " + arn);
        } else {
            CreateUserRequest request = new CreateUserRequest().withPath("/customers/").withUserName(userName);
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            CreateUserResult result = retry.execute(context -> iam.createUser(request));
            arn = result.getUser().getArn();
        }
        AddUserToGroupRequest request = new AddUserToGroupRequest().withUserName(userName).withGroupName(customerGroup);
        iam.addUserToGroup(request);
        log.info("Added user " + userName + " to group " + customerGroup);
        return arn;
    }

    @Override
    public AccessKey createCustomerKey(String userName) {
        if (hasCustomerUser(userName)) {
            AmazonIdentityManagement iam = iamClient();
            CreateAccessKeyRequest request = new CreateAccessKeyRequest().withUserName(userName);
            CreateAccessKeyResult result = iam.createAccessKey(request);
            return result.getAccessKey();
        } else {
            throw new IllegalArgumentException("User " + userName + " does not exists");
        }
    }

    @Override
    public boolean hasCustomerKey(String userName) {
        boolean hasKey = false;
        if (hasCustomerUser(userName)) {
            AmazonIdentityManagement iam = iamClient();
            ListAccessKeysRequest request = new ListAccessKeysRequest().withUserName(userName);
            ListAccessKeysResult result = iam.listAccessKeys(request);
            hasKey = CollectionUtils.isNotEmpty(result.getAccessKeyMetadata());
        }
        return hasKey;
    }

    @Override
    public void deleteCustomerUser(String userName) {
        if (hasCustomerUser(userName)) {
            removeUserGroups(userName);
            removeAccessKeys(userName);
            AmazonIdentityManagement iam = iamClient();
            RetryTemplate retry = RetryUtils.getRetryTemplate(3, null, //
                    Collections.singleton(NoSuchEntityException.class));
            try {
                DeleteUserResult result = retry.execute(context -> {
                    DeleteUserRequest request = new DeleteUserRequest().withUserName(userName);
                    int count = context.getRetryCount();
                    if (count > 0) {
                        log.info("(Attempt=" + (count + 1) + ") Deleting user " + userName);
                    }
                    return iam.deleteUser(request);
                });
                String requestId = result.getSdkResponseMetadata().getRequestId();
                log.info("Deleted user " + userName + " with AWS request-id " + requestId);
            } catch (NoSuchEntityException e) {
                log.info("User " + userName + " seems already deleted.");
            }
        } else {
            log.info("User " + userName + " does not exist, skip deleting.");
        }
    }

    @Override
    public String getUserPolicy(String userName, String policyName) {
        AmazonIdentityManagement iam = iamClient();
        GetUserPolicyResult result;
        GetUserPolicyRequest request = new GetUserPolicyRequest().withUserName(userName).withPolicyName(policyName);
        try {
            result = iam.getUserPolicy(request);
        } catch (NoSuchEntityException e) {
            log.info("Cannot find policy " + policyName + " for user " + userName);
            return null;
        }
        try {
            return URLDecoder.decode(result.getPolicyDocument(), "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse policy document: " + result.getPolicyDocument(), e);
        }
    }

    @Override
    public void putUserPolicy(String userName, String policyName, String policyDocument) {
        AmazonIdentityManagement iam = iamClient();
        PutUserPolicyRequest request = new PutUserPolicyRequest() //
                .withUserName(userName) //
                .withPolicyName(policyName) //
                .withPolicyDocument(policyDocument);
        RetryTemplate retry = RetryUtils.getRetryTemplate(3, null, //
                Collections.singleton(NoSuchEntityException.class));
        retry.execute(context -> iam.putUserPolicy(request));
        log.info("Added inline policy " + policyName + " to user " + userName + ": " + policyDocument);
    }

    private void removeUserGroups(String userName) {
        AmazonIdentityManagement iam = iamClient();
        RetryTemplate retry = RetryUtils.getRetryTemplate(3, null, //
                Collections.singleton(NoSuchEntityException.class));
        ListGroupsForUserRequest listGrpRequest = new ListGroupsForUserRequest().withUserName(userName);
        ListGroupsForUserResult listGrpResult = retry.execute(context -> iam.listGroupsForUser(listGrpRequest));
        if (CollectionUtils.isNotEmpty(listGrpResult.getGroups())) {
            listGrpResult.getGroups().forEach(grp -> {
                RemoveUserFromGroupRequest request = new RemoveUserFromGroupRequest() //
                        .withUserName(userName).withGroupName(grp.getGroupName());
                iam.removeUserFromGroup(request);
            });
        }
    }

    private void removeAccessKeys(String userName) {
        AmazonIdentityManagement iam = iamClient();
        RetryTemplate retry = RetryUtils.getRetryTemplate(3, null, //
                Collections.singleton(NoSuchEntityException.class));
        ListAccessKeysRequest listKeyRequest = new ListAccessKeysRequest().withUserName(userName);
        ListAccessKeysResult listKeyResult = retry.execute(context -> iam.listAccessKeys(listKeyRequest));
        if (CollectionUtils.isNotEmpty(listKeyResult.getAccessKeyMetadata())) {
            listKeyResult.getAccessKeyMetadata().forEach(accessKeyMetadata -> {
                DeleteAccessKeyRequest deleteKeyRequest = new DeleteAccessKeyRequest() //
                        .withUserName(userName).withAccessKeyId(accessKeyMetadata.getAccessKeyId());
                iam.deleteAccessKey(deleteKeyRequest);
            });
        }
    }

    public void deleteUserPolicy(String userName, String policyName) {
        AmazonIdentityManagement iam = iamClient();
        DeleteUserPolicyRequest request = new DeleteUserPolicyRequest() //
                .withUserName(userName) //
                .withPolicyName(policyName);
        RetryTemplate retry = RetryUtils.getRetryTemplate(3, null, //
                Collections.singleton(NoSuchEntityException.class));
        try {
            retry.execute(context -> iam.deleteUserPolicy(request));
        } catch (NoSuchEntityException e) {
            log.warn("Cannot find policy " + policyName + " for user " + userName + ": " + e.getMessage());
        }
    }


    public boolean hasCustomerUser(String username) {
        return StringUtils.isNotBlank(getCustomerUserArn(username));
    }

    public String getCustomerUserArn(String username) {
        AmazonIdentityManagement iam = iamClient();
        GetUserRequest request = new GetUserRequest().withUserName(username);
        try {
            GetUserResult result = iam.getUser(request);
            return result.getUser().getArn();
        } catch (NoSuchEntityException e) {
            return null;
        }
    }

    private AmazonIdentityManagement iamClient() {
        if (iamClient == null) {
            synchronized (this) {
                if (iamClient == null) {
                    iamClient = AmazonIdentityManagementClientBuilder.standard() //
                            .withCredentials(new AWSStaticCredentialsProvider(customerCredentials)) //
                            .withRegion(Regions.fromName(region)) //
                            .build();
                }
            }
        }
        return iamClient;
    }

    private RetryTemplate getRetryTemplate() {
        RetryTemplate retry = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(2000);
        backOffPolicy.setMultiplier(2.0);
        retry.setBackOffPolicy(backOffPolicy);
        retry.setThrowLastExceptionOnExhausted(true);
        return retry;
    }

}
