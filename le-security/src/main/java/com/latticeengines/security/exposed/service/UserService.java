package com.latticeengines.security.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.security.exposed.AccessLevel;

public interface UserService {

    boolean addAdminUser(UserRegistrationWithTenant userRegistrationWithTenant);

    boolean addAdminUser(String createdByUser, UserRegistrationWithTenant userRegistrationWithTenant);

    boolean createUser(String userName, UserRegistration userRegistration);

    boolean upsertSamlIntegrationUser(String userName, LoginValidationResponse samlLoginResp, String tenantDeploymentId);

    RegistrationResult registerUserToTenant(String userName, UserRegistrationWithTenant userRegistrationWithTenant);

    User findByEmail(String email);

    User findByUsername(String username);

    boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username);

    boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username, String createdByUser,
                              Long expirationDate, boolean createUser);

    boolean resignAccessLevel(String tenantId, String username);

    AccessLevel getAccessLevel(String tenantId, String username);

    boolean deleteUser(String tenantId, String username);

    List<User> getUsers(String tenantId);

    List<User> getUsers(String tenantId, UserFilter filter);

    boolean isSuperior(AccessLevel loginLevel, AccessLevel targetLevel);

    boolean inTenant(String tenantId, String username);

    boolean updateCredentials(User user, UserUpdateData data);

    boolean updateClearTextCredentials(User user, UserUpdateData data);

    String getURLSafeUsername(String username);

    String deactiveUserStatus(String userName, String emails);

    GlobalAuthUser findByEmailNoJoin(String username);

    boolean deleteUserByEmail(String email);

    String addUserAccessLevel(String userName, String emails, AccessLevel level);

    RegistrationResult registerUserWithNoTenant(UserRegistration userReg);

}
