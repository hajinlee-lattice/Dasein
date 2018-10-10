package com.latticeengines.aws.iam;

import com.amazonaws.services.identitymanagement.model.AccessKey;
import com.amazonaws.services.identitymanagement.model.AccessKeyMetadata;

public interface IAMService {

    // return IAM User ARN
    String createCustomerUser(String userName);

    void deleteCustomerUser(String userName);

    AccessKey createCustomerKey(String userName);

    AccessKey refreshCustomerKey(String userName);

    boolean hasCustomerKey(String userName);

    AccessKeyMetadata getCustomerKeyIfExists(String userName);

    String getUserPolicy(String userName, String policyName);

    void putUserPolicy(String userName, String policyName, String policyDocument);

    void deleteUserPolicy(String userName, String policyName);
}
