package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;

public interface UserService {

    Boolean addAdminUser(UserRegistrationWithTenant userRegistrationWithTenant);
    
}
