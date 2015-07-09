package com.latticeengines.security.exposed.service.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.exception.LoginException;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;

@Component("userService")
public class UserServiceImpl implements UserService {
    private static final Log LOGGER = LogFactory.getLog(UserServiceImpl.class);

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

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

        userRegistration.toLowerCase();
        User user = userRegistration.getUser();
        Credentials creds = userRegistration.getCredentials();

        User userByEmail = globalUserManagementService.getUserByEmail(user.getEmail());

        if (userByEmail != null) {
            LOGGER.warn(String.format(
                    "A user with the same email address %s already exists. Please update instead of create user.",
                    userByEmail));
        } else {
            try {
                globalUserManagementService.registerUser(user, creds);
                userByEmail = globalUserManagementService.getUserByEmail(user.getEmail());
            } catch (Exception e) {
                LOGGER.warn("Error creating admin user.", e);
                globalUserManagementService.deleteUser(user.getUsername());
                globalUserManagementService.deleteUser(user.getEmail());
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
        return AccessLevel.findAccessLevel(rights);
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
    public List<User> getUsers(String tenantId, UserFilter filter) {
        List<User> users = new ArrayList<>();
        try {
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList
                    = globalUserManagementService.getAllUsersOfTenant(tenantId);
            for (Map.Entry<User, List<String>> userRights : userRightsList) {
                User user = userRights.getKey();
                AccessLevel accessLevel = AccessLevel.findAccessLevel(userRights.getValue());
                if (accessLevel != null) {
                    user.setAccessLevel(accessLevel.name());
                }
                if (filter.visible(user)) users.add(user);
            }
        } catch (LedpException e) {
            LOGGER.warn(String.format("Trying to get all users from a non-existing tenant %s", tenantId), e);
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
            throw e;
        }

        return users;
    }

    @Override
    public List<User> getUsers(String tenantId) {
        return getUsers(tenantId, UserFilter.TRIVIAL_FILTER);
    }

    @Override
    public boolean isSuperior(AccessLevel loginLevel, AccessLevel targetLevel) {
        return loginLevel != null && targetLevel != null && targetLevel.compareTo(loginLevel) <= 0;
    }

    @Override
    public boolean inTenant(String tenantId, String username) {
        return !globalUserManagementService.getRights(username, tenantId).isEmpty();
    }

    @Override
    public String getURLSafeUsername(String username) {
        if (username.endsWith("\"") && username.startsWith("\""))
            return username.substring(1, username.length() - 1);
        return username;
    }

    @Override
    public RegistrationResult registerUserToTenant(UserRegistrationWithTenant userRegistrationWithTenant) {
        UserRegistration userRegistration = userRegistrationWithTenant.getUserRegistration();
        userRegistration.toLowerCase();
        User user = userRegistration.getUser();
        String tenantId = userRegistrationWithTenant.getTenant();

        RegistrationResult result = validateNewUser(user, tenantId);
        if (!result.isValid()) return result;

        result.setValid(createUser(userRegistration));
        if (!result.isValid()) return result;

        if (StringUtils.isNotEmpty(user.getAccessLevel())) {
            assignAccessLevel(AccessLevel.valueOf(user.getAccessLevel()), tenantId, user.getUsername());
        }

        String tempPass = globalUserManagementService.resetLatticeCredentials(user.getUsername());
        result.setPassword(tempPass);

        return result;
    }

    @Override
    public boolean updateCredentials(User user, UserUpdateData data) {
        // change password
        String oldPassword = data.getOldPassword();
        String newPassword = data.getNewPassword();
        if (oldPassword != null && newPassword != null) {
            Credentials oldCreds = new Credentials();
            oldCreds.setUsername(user.getUsername());
            oldCreds.setPassword(oldPassword);

            Credentials newCreds = new Credentials();
            newCreds.setUsername(user.getUsername());
            newCreds.setPassword(newPassword);

            Ticket ticket;
            try {
                ticket = globalAuthenticationService.authenticateUser(user.getUsername(), oldPassword);
            } catch (LedpException e) {
                throw new LoginException(e);
            }

            if (globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds)) {
                LOGGER.info(String.format("%s changed his/her password", user.getUsername()));
                return true;
            }
        }
        return false;
    }

    private RegistrationResult validateNewUser(User newUser, String tenantId) {
        String email = newUser.getEmail();
        User oldUser = findByEmail(email);
        RegistrationResult result = new RegistrationResult();
        result.setValid(true);

        if (oldUser != null) {
            result.setValid(false);
            if (!inTenant(tenantId, oldUser.getUsername())) {
                result.setConflictingUser(oldUser);
            }
            return result;
        }

        String username = newUser.getUsername();
        oldUser = findByUsername(username);
        if (oldUser != null) {
            result.setValid(false);
            if (!inTenant(tenantId, oldUser.getUsername())) {
                result.setConflictingUser(oldUser);
            }
            return result;
        }

        return result;
    }

    private boolean softDelete(String tenantId, String username) {
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
}
