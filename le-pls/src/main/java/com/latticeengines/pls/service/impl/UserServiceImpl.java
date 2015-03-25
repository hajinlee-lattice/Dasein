package com.latticeengines.pls.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.service.UserService;

@Component("userService")
public class UserServiceImpl implements UserService {
    private static final Log log = LogFactory.getLog(UserServiceImpl.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Override
    public Boolean addAdminUser(UserRegistrationWithTenant userRegistrationWithTenant) {
        UserRegistration userRegistration = userRegistrationWithTenant.getUserRegistration();
        String tenant = userRegistrationWithTenant.getTenant();

        if (userRegistration == null) {
            log.error("User registration cannot be null.");
            return false;
        }
        if (userRegistration.getUser() == null) {
            log.error("User cannot be null.");
            return false;
        }
        if (userRegistration.getCredentials() == null) {
            log.error("Credentials cannot be null.");
            return false;
        }
        if (tenant == null) {
            log.error("Tenant cannot be null.");
            return false;
        }

        try {
            globalUserManagementService.registerUser(userRegistration.getUser(), userRegistration.getCredentials());
        } catch (Exception e) {
            log.warn("Error creating admin user.", e);
        }

        for (GrantedRight right : GrantedRight.getAdminRights()) {
            String username = userRegistration.getUser().getUsername();
            try {
                globalUserManagementService.grantRight(right.getAuthority(), tenant, username);
            } catch (Exception e) {
                log.warn(String.format("Error granting right %s to user %s.", right.getAuthority(), username), e);
            }
        }

        return globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail()) != null;
    }

}
