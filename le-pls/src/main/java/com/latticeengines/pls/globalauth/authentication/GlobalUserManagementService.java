package com.latticeengines.pls.globalauth.authentication;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;

public interface GlobalUserManagementService {

    Boolean registerUser(User user, Credentials creds);
    
    Boolean grantRight(String right, String tenant, String username);

    Boolean revokeRight(String right, String tenant, String username);

    Boolean forgotLatticeCredentials(String username, String tenantId);

    Boolean modifyLatticeCredentials(Ticket ticket, Credentials oldCreds, Credentials newCreds);
    
    Boolean deleteUser(String username);
}
