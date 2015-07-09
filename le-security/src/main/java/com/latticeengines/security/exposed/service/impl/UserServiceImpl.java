package com.latticeengines.security.exposed.service.impl;

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
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.UserService;

@Component("userService")
public class UserServiceImpl implements UserService {
    private static final Log LOGGER = LogFactory.getLog(UserServiceImpl.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private SessionService sessionService;

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
            LOGGER.warn(String.format(
                    "A user with the same email address %s already exists. Update instead of create user.",
                    userByEmail));
        } else {
            try {
                globalUserManagementService.registerUser(userRegistration.getUser(), userRegistration.getCredentials());
            } catch (Exception e) {
                LOGGER.warn("Error creating admin user.", e);
            }
        }

        String username = userRegistration.getUser().getUsername();
        assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant, username);

        return globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail()) != null;
    }

    @Override
    public boolean createUser(UserRegistration userRegistration) {
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

        User userByEmail = globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail());

        if (userByEmail != null) {
            LOGGER.warn(String.format(
                    "A user with the same email address %s already exists. Please update instead of create user.",
                    userByEmail));
        } else {
            try {
                globalUserManagementService.registerUser(userRegistration.getUser(), userRegistration.getCredentials());
                userByEmail = globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail());
            } catch (Exception e) {
                LOGGER.warn("Error creating admin user.", e);
            }
        }

        return userByEmail != null;
    }

    @Override
    public boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username) {
        if (accessLevel == null) { return resignAccessLevel(tenantId, username); }
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
                        String.format("Error resigning access level %s from user %s.",
                                accessLevel.name(), username),
                        e);
            }
        }
        return success;
    }

    @Override
    public AccessLevel getAccessLevel(String tenantId, String username) {
        List<String> rights = globalUserManagementService.getRights(username, tenantId);
        AccessLevel toReturn = AccessLevel.findAccessLevel(rights);
        if (toReturn == null && hasPLSRights(rights)) {
            User user = globalUserManagementService.getUserByUsername(username);
            toReturn = sessionService.upgradeFromDeprecatedBARD(
                    tenantId, username, user.getEmail(), rights);
        }
        return  toReturn;
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

    @Override
    public User findByEmail(String email) { return globalUserManagementService.getUserByEmail(email); }

    @Override
    public User findByUsername(String username) {
        return globalUserManagementService.getUserByUsername(username);
    }

    @Override
    public List<User> getUsers(String tenantId) {
        List<User> users = new ArrayList<>();
        try {
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList
                    = globalUserManagementService.getAllUsersOfTenant(tenantId);
            for (Map.Entry<User, List<String>> userRights : userRightsList) {
                User user = userRights.getKey();
                AccessLevel accessLevel = AccessLevel.findAccessLevel(userRights.getValue());
                if (accessLevel == null) {
                    accessLevel = getAccessLevel(tenantId, user.getUsername());
                }
                if (accessLevel != null) {
                    user.setAccessLevel(accessLevel.name());
                }
                users.add(user);
            }
        } catch (LedpException e) {
            LOGGER.warn(String.format("Trying to get all users from a non-existing tenant %s", tenantId), e);
        }

        return users;
    }

    @Override
    public boolean isVisible(AccessLevel loginLevel, AccessLevel targetLevel) {
        return (loginLevel != null && targetLevel != null)
                && (loginLevel.compareTo(AccessLevel.INTERNAL_USER) >= 0
                || targetLevel.compareTo(AccessLevel.INTERNAL_USER) < 0);
    }

    @Override
    public boolean isSuperior(AccessLevel loginLevel, AccessLevel targetLevel) {
        return loginLevel != null && targetLevel != null && targetLevel.compareTo(loginLevel) <= 0;
    }

    @Override
    public boolean inTenant(String tenantId, String username) {
        try {
            return !globalUserManagementService.getRights(username, tenantId).isEmpty();
        } catch (LedpException e) {
            return false;
        }
    }

    @Override
    public String getURLSafeUsername(String username) {
        if (username.endsWith("\"") && username.startsWith("\""))
            return username.substring(1, username.length() - 1);
        return username;
    }

    private boolean hasPLSRights(List<String> rights) {
        for (String right : rights) {
            if (right.contains("_PLS_")) { return true; }
        }
        return false;
    }
}
