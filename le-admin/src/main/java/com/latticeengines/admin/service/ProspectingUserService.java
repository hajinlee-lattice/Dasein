package com.latticeengines.admin.service;

import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.security.UserRegistration;

public interface ProspectingUserService {
    RegistrationResult createUser(UserRegistration userReg);
}
