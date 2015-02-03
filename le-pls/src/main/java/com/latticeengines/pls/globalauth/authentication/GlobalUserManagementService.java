package com.latticeengines.pls.globalauth.authentication;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;

public interface GlobalUserManagementService {

    Boolean registerUser(User user, Credentials creds);
    
    Boolean grantRight(String user, String tenant, String right);

    Boolean revokeRight(String right, String tenant, String username);
}
