package com.latticeengines.aws.iam;

import com.amazonaws.services.identitymanagement.model.AccessKey;

public interface IAMService {

    // return ARN
    String createCustomerUser(String userName);

    void deleteCustomerUser(String userName);

    AccessKey createCustomerKey(String userName);

    boolean hasCustomerKey(String userName);

    String getUserPolicy(String userName, String policyName);

    void putUserPolicy(String userName, String policyName, String policyDocument);

    void deleteUserPolicy(String userName, String policyName);
}
