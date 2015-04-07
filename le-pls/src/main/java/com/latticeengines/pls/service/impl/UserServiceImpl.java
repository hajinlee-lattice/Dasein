package com.latticeengines.pls.service.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.AccessLevel;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.service.UserService;

@Component("userService")
public class UserServiceImpl implements UserService {
    private static final Log LOGGER = LogFactory.getLog(UserServiceImpl.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Override
    public boolean addAdminUser(UserRegistrationWithTenant userRegistrationWithTenant) {
        UserRegistration userRegistration = userRegistrationWithTenant.getUserRegistration();
        String tenant = userRegistrationWithTenant.getTenant();

        if (userRegistration == null) {
            LOGGER.error("User registration cannot be null.");
            return false;
        }

        if (userRegistration.getUser() == null) {
            LOGGER.error("User cannot be null.");
            return false;
        }
        if (userRegistration.getCredentials() == null) {
            LOGGER.error("Credentials cannot be null.");
            return false;
        }
        if (tenant == null) {
            LOGGER.error("Tenant cannot be null.");
            return false;
        }

        User userByEmail = globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail());

        if (userByEmail != null) {
            LOGGER.error(String.format("A user with the same email address %s already exists.", userByEmail));
            return false;
        }

        try {
            globalUserManagementService.registerUser(userRegistration.getUser(), userRegistration.getCredentials());
        } catch (Exception e) {
            LOGGER.warn("Error creating admin user.", e);
        }


        String username = userRegistration.getUser().getUsername();
        assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant, username);

        return globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail()) != null;
    }

    @Override
    public boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username) {
        if (!accessLevel.equals(getAccessLevel(tenantId, username)) && resignAccessLevel(tenantId, username)) {
            try {
                return globalUserManagementService.grantRight(accessLevel.name(), tenantId, username);
            } catch (Exception e) {
                LOGGER.warn(
                        String.format("Error assigning access level %s to user %s.", accessLevel.name(), username), e);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean resignAccessLevel(String tenantId, String username) {
        List<String> rights = globalUserManagementService.getRights(username, tenantId);
        boolean success = true;
        for (AccessLevel accessLevel : AccessLevel.values()) {
            try {
                if (rights.contains(accessLevel.name())) {
                    success = success
                            && globalUserManagementService.revokeRight(accessLevel.name(), tenantId, username);
                }
            } catch (Exception e) {
                LOGGER.warn(
                        String.format("Error resigning access level %s from user %s.", accessLevel.name(), username),
                        e);
            }
        }
        return success;
    }

    @Override
    public AccessLevel getAccessLevel(String tenantId, String username) {
        List<String> rights = globalUserManagementService.getRights(username, tenantId);
        AccessLevel toReturn = getAccessLevel(rights);
        if (rights.size() > 1 && !username.equals("admin")) {
            if (toReturn == null) {
                List<GrantedRight> grantedRights = new ArrayList<>();
                for (String right: rights) {
                    GrantedRight grantedRight = GrantedRight.getGrantedRight(right);
                    if (grantedRight != null) { grantedRights.add(grantedRight); }
                }
                toReturn = AccessLevel.maxAccessLevel(grantedRights);
            }
            softDelete(tenantId, username);
            assignAccessLevel(toReturn, tenantId, username);
        }
        return toReturn;
    }

    @Override
    public AccessLevel getAccessLevel(List<String> rights) {
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
            boolean success = true;
            List<String> rights = globalUserManagementService.getRights(username, tenantId);
            if (!rights.isEmpty()) {
                for (GrantedRight right : AccessLevel.SUPER_ADMIN.getGrantedRights()) {
                    try {
                        if (rights.contains(right.getAuthority())) {
                            success = success
                                    && globalUserManagementService.revokeRight(
                                    right.getAuthority(), tenantId, username);
                        }
                    } catch (Exception e) {
                        //ignore
                    }
                }
            }
            return success;
        } else {
            return false;
        }
    }

    @Override
    public boolean deleteUser(String tenantId, String username) {
        if (softDelete(tenantId, username) && globalUserManagementService.isRedundant(username)) {
            return globalUserManagementService.deleteUser(username);
        }
        return false;
    }

    /**
     *
     * @param tenantId
     * @return all users in the tenant except admin:admin
     */
    @Override
    public List<User> getUsers(String tenantId) {
        List<User> users = new ArrayList<>();
        try {
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList
                    = globalUserManagementService.getAllUsersOfTenant(tenantId);
            for (Map.Entry<User, List<String>> userRights : userRightsList) {
                User user = userRights.getKey();
                if (!user.getUsername().equals("admin")) {
                    AccessLevel accessLevel = getAccessLevel(userRights.getValue());
                    user.setAccessLevel(accessLevel.name());
                    users.add(user);
                }
            }
        } catch (LedpException e) {
            LOGGER.warn(String.format("Trying to get all users from a non-existing tenant %s", tenantId), e);
        }

        return users;
    }

    @Override
    public boolean isVisible(AccessLevel loginLevel, AccessLevel targetLevel) {
        if (targetLevel == null) { return false; }
        if (loginLevel == AccessLevel.EXTERNAL_ADMIN) { return targetLevel.equals(AccessLevel.EXTERNAL_USER); }
        return targetLevel.compareTo(loginLevel) <= 0;
    }
}
