package com.latticeengines.pls.globalauth.authentication;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;

import java.util.AbstractMap;
import java.util.List;

public interface GlobalUserManagementService {

    Boolean registerUser(User user, Credentials creds);
    
    Boolean grantRight(String right, String tenant, String username);

    Boolean revokeRight(String right, String tenant, String username);

    Boolean forgotLatticeCredentials(String username, String tenantId);

    Boolean modifyLatticeCredentials(Ticket ticket, Credentials oldCreds, Credentials newCreds);

    String resetLatticeCredentials(String username);

    Boolean deleteUser(String username);

    User getUserByEmail(String email);

    Boolean isRedundant(String username);

    List<AbstractMap.SimpleEntry<User, List<String>>> getAllUsersOfTenant(String tenantId);
}
