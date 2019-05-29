package com.latticeengines.security.exposed.globalauth;

import java.util.AbstractMap;
import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;

public interface GlobalUserManagementService {

    Boolean existExpireDateChanged(String username, String tenant, String right, Long expirationDate);

    Boolean registerUser(String userName, User user, Credentials creds);

    Boolean registerExternalIntegrationUser(String userName, User user);

    Boolean grantRight(String right, String tenant, String username);

    Boolean grantRight(String right, String tenant, String username, String createdByUser, Long expirationDate);

    Boolean revokeRight(String right, String tenant, String username);

    List<String> getRights(String username, String tenantId);

    Boolean forgotLatticeCredentials(String username);

    Boolean modifyLatticeCredentials(Ticket ticket, Credentials oldCreds, Credentials newCreds);

    Boolean modifyClearTextLatticeCredentials(Ticket ticket, Credentials oldCreds, Credentials newCreds);

    String resetLatticeCredentials(String username);

    Boolean deleteUser(String username);

    User getUserByEmail(String email);

    User getUserByUsername(String username);

    Boolean isRedundant(String username);

    void checkRedundant(String username);

    List<AbstractMap.SimpleEntry<User, List<String>>> getAllUsersOfTenant(String tenantId);

    String deactiveUserStatus(String userName, String emails);

    GlobalAuthUser findByEmailNoJoin(String email);

    boolean deleteUserByEmail(String email);

    String addUserAccessLevel(String userName, String emails, AccessLevel level);

}
