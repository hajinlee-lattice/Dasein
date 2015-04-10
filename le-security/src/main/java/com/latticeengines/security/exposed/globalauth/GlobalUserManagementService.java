package com.latticeengines.security.exposed.globalauth;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;

import java.util.AbstractMap;
import java.util.List;

public interface GlobalUserManagementService {

    Boolean registerUser(User user, Credentials creds);
    
    Boolean grantRight(String right, String tenant, String username);

    Boolean revokeRight(String right, String tenant, String username);

    List<String> getRights(String username, String tenantId);

    Boolean forgotLatticeCredentials(String username);

    Boolean modifyLatticeCredentials(Ticket ticket, Credentials oldCreds, Credentials newCreds);

    String resetLatticeCredentials(String username);

    Boolean deleteUser(String username);

    User getUserByEmail(String email);

    User getUserByUsername(String username);

    Boolean isRedundant(String username);

    List<AbstractMap.SimpleEntry<User, List<String>>> getAllUsersOfTenant(String tenantId);
}
