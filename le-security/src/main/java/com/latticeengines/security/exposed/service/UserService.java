package com.latticeengines.security.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.security.exposed.AccessLevel;

public interface UserService {

    boolean addAdminUser(UserRegistrationWithTenant userRegistrationWithTenant);

    boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username);

    boolean resignAccessLevel(String tenantId, String username);

    AccessLevel getAccessLevel(String tenantId, String username);

    boolean softDelete(String tenantId, String username);

    boolean deleteUser(String tenantId, String username);

    List<User> getUsers(String tenantId);

    boolean isVisible(AccessLevel loginLevel, AccessLevel targetLevel);

    boolean isSuperior(AccessLevel loginLevel, AccessLevel targetLevel);

    boolean inTenant(String tenantId, String username);
}
