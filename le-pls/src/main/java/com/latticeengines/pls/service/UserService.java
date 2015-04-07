package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.security.AccessLevel;

public interface UserService {

    boolean addAdminUser(UserRegistrationWithTenant userRegistrationWithTenant);

    boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username);

    boolean resignAccessLevel(String tenantId, String username);

    AccessLevel getAccessLevel(String tenantId, String username);

    AccessLevel getAccessLevel(List<String> rights);

    boolean softDelete(String tenantId, String username);

    boolean deleteUser(String tenantId, String username);

    List<User> getUsers(String tenantId);

    boolean isVisible(AccessLevel loginLevel, AccessLevel targetLevel);

    boolean inTenant(String tenantId, String username);
}
