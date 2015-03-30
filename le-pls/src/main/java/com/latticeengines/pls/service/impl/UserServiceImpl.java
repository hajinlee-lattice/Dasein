package com.latticeengines.pls.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.AccessLevel;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.service.UserService;

@Component("userService")
public class UserServiceImpl implements UserService {
    private static final Log log = LogFactory.getLog(UserServiceImpl.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Override
    public boolean addAdminUser(UserRegistrationWithTenant userRegistrationWithTenant) {
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

        User userByEmail = globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail());

        if (userByEmail != null) {
            log.error(String.format("A user with the same email address %s already exists.", userByEmail));
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

    @Override
    public boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username) {
        if (resignAccessLevel(tenantId, username)) {
            try {
                return globalUserManagementService.grantRight(accessLevel.name(), tenantId, username);
            } catch (Exception e) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean resignAccessLevel(String tenantId, String username) {
        boolean success = true;
        for (AccessLevel accessLevel : AccessLevel.values()) {
            try {
                success = success && globalUserManagementService.revokeRight(accessLevel.name(), tenantId, username);
            } catch (Exception e) {
                //ignore
            }
        }
        return success;
    }

    @Override
    public AccessLevel getAccessLevel(String tenantId, String username) {
        List<String> rights = globalUserManagementService.getRights(username, tenantId);
        AccessLevel toReturn = null;
        for (String right : rights) {
            try {
                AccessLevel accessLevel = AccessLevel.valueOf(right);
                if (toReturn == null || accessLevel.compareTo(toReturn) > 0) {
                    toReturn = accessLevel;
                }
            } catch (IllegalArgumentException e) {
                //ignore
            }
        }
        return toReturn;
    }

    @Override
    public boolean softDelete(String tenantId, String username) {
        if (resignAccessLevel(tenantId, username)) {
            //TODO:song this is temporary until the concept of GrantedRight no longer exists in GA
            boolean success = true;
            for (GrantedRight right : AccessLevel.SUPER_ADMIN.getGrantedRights()) {
                try {
                    success = success && globalUserManagementService.revokeRight(right.getAuthority(), tenantId, username);
                } catch (Exception e) {
                    //ignore
                }
            }
            return success;
        } else {
            return false;
        }
    }
}
