package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.security.AccessLevel;

public interface UserService {

    boolean addAdminUser(UserRegistrationWithTenant userRegistrationWithTenant);

    boolean grantAccessLevel(AccessLevel accessLevel, String tenantId, String username);

    boolean revokeAccessLevel(String tenantId, String username);

    AccessLevel getAccessLevel(String tenantId, String username);

}
