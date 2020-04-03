package com.latticeengines.security.exposed.service.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.EmailValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.service.GlobalTeamManagementService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.util.IntegrationUserUtils;

@Component("userService")
public class UserServiceImpl implements UserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserServiceImpl.class);

    @Inject
    private GlobalUserManagementService globalUserManagementService;

    @Inject
    private GlobalAuthenticationService globalAuthenticationService;

    @Inject
    private GlobalTenantManagementService globalTenantManagementService;

    @Inject
    private GlobalSessionManagementService globalSessionManagementService;

    @Inject
    private GlobalTeamManagementService globalTeamManagementService;

    private static EmailValidator emailValidator = EmailValidator.getInstance();

    private ExecutorService clearSessionService = ThreadPoolUtils.getCachedThreadPool("clear-session");

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
                globalUserManagementService.registerUser(null, userRegistration.getUser(),
                        userRegistration.getCredentials());
            } catch (Exception e) {
                LOGGER.warn("Error creating admin user.");
            }
        }

        String username = userRegistration.getUser().getUsername();
        assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant, username);

        return globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail()) != null;
    }

    @Override
    public boolean addAdminUser(String createdByUser, UserRegistrationWithTenant userRegistrationWithTenant) {
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
                globalUserManagementService.registerUser(createdByUser, userRegistration.getUser(),
                        userRegistration.getCredentials());
            } catch (Exception e) {
                LOGGER.warn("Error creating admin user.");
            }
        }

        String username = userRegistration.getUser().getUsername();
        assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant, username, createdByUser, null, true);

        return globalUserManagementService.getUserByEmail(userRegistration.getUser().getEmail()) != null;
    }

    @Override
    public boolean createUser(String userName, UserRegistration userRegistration) {
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
                globalUserManagementService.registerUser(userName, user, creds);
                userByEmail = globalUserManagementService.getUserByEmail(user.getEmail());
            } catch (Exception e) {
                LOGGER.warn("Error creating admin user.");
                globalUserManagementService.deleteUser(user.getUsername());
                globalUserManagementService.deleteUser(user.getEmail());
            }
        }

        return userByEmail != null;
    }

    @Override
    public boolean upsertSamlIntegrationUser(String userName, LoginValidationResponse samlLoginResp,
            String tenantDeploymentId) {
        GlobalAuthUser globalAuthUser = globalUserManagementService.findByEmailNoJoin(samlLoginResp.getUserId());
        User userInfoFromSaml = IntegrationUserUtils.buildUserFrom(samlLoginResp);
        if (globalAuthUser == null) {
            LOGGER.info("Creating new User: {} for Tenant: {}", samlLoginResp.getUserId(), tenantDeploymentId);
            globalUserManagementService.registerExternalIntegrationUser(userName, userInfoFromSaml);
        }
        List<String> gaUserRights = globalUserManagementService.getRights(userInfoFromSaml.getEmail(),
                tenantDeploymentId);

        AccessLevel grantAccessLevel = null;
        if (StringUtils.isBlank(userInfoFromSaml.getAccessLevel())) {
            // If the SAML role doesn't match with any of the allowed roles,
            // then grant External User role
            assignAccessLevel(AccessLevel.EXTERNAL_USER, tenantDeploymentId, userInfoFromSaml.getEmail());
        } else {
            // If there is any change in user configurations between login
            // attempts, update the access level
            AccessLevel existingAccessLevel = AccessLevel.findAccessLevel(gaUserRights);
            AccessLevel samlResponseAccessLevel = AccessLevel
                    .findAccessLevel(Collections.singletonList(userInfoFromSaml.getAccessLevel()));

            if (samlResponseAccessLevel != existingAccessLevel) {
                assignAccessLevel(samlResponseAccessLevel, tenantDeploymentId, userInfoFromSaml.getEmail());
            }
        }
        return true;
    }

    @Override
    public boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username) {
        if (accessLevel == null) {
            return resignAccessLevel(tenantId, username);
        }
        if (!accessLevel.equals(getAccessLevel(tenantId, username)) && resignAccessLevel(tenantId, username)) {
            try {
                return globalUserManagementService.grantRight(accessLevel.name(), tenantId, username);
            } catch (Exception e) {
                LOGGER.warn(String.format("Error assigning access level %s to user %s.", accessLevel.name(), username));
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username, String createdByUser,
            Long expirationDate, boolean createUser, boolean clearSession, List<String> userTeamIds) {
        if (accessLevel == null) {
            return resignAccessLevel(tenantId, username);
        }
        // make sure only internal user has expiration date
        if (!AccessLevel.getInternalAccessLevel().contains(accessLevel)) {
            expirationDate = null;
        }

        // if loginUser is not super admin

        // remove comparing user with same access level to tenant in different
        // update times as user with same access level can be expiration date

        List<String> rights = globalUserManagementService.getRights(createdByUser, tenantId);
        if (!rights.contains(AccessLevel.SUPER_ADMIN.name())) {
            // only super admin user can add expire after data when creating user, other
            // wise expire after data should be null
            if (createUser) {
                expirationDate = null;
            } else {
                if (globalUserManagementService.existExpireDateChanged(username, tenantId, accessLevel.name(),
                        expirationDate)) {
                    throw new AccessDeniedException("Access denied.");
                }
            }
        }
        List<GlobalAuthUserTenantRight> rightsData = globalUserManagementService.getUserRightsByUsername(username, tenantId, true);
        List<String> originalRights = globalUserManagementService.getRights(rightsData);
        if (resignAccessLevel(tenantId, username, originalRights)) {
            try {
                List<GlobalAuthTeam> globalAuthTeams = new ArrayList<>();
                if (userTeamIds == null && CollectionUtils.isNotEmpty(rightsData)) {
                    globalAuthTeams = rightsData.get(0).getGlobalAuthTeams();
                } else if (CollectionUtils.isNotEmpty(userTeamIds)) {
                    globalAuthTeams = globalTeamManagementService.getTeamsByTeamIds(userTeamIds, false);
                }
                boolean result = globalUserManagementService.grantRight(accessLevel.name(), tenantId, username,
                        createdByUser, expirationDate, globalAuthTeams);
                if (result && clearSession) {
                    AccessLevel originalLevel = AccessLevel.findAccessLevel(originalRights);
                    Long userId = findIdByUsername(username);
                    if (!isSuperior(accessLevel, originalLevel)) {
                        clearSession(tenantId, Collections.singletonList(userId));
                    } else {
                        if (userTeamIds != null) {
                            List<String> orgTeamIds = globalUserManagementService.getTeamIds(rightsData);
                            if (teamIdsChanged(userTeamIds, orgTeamIds)) {
                                clearSession(false, tenantId, Collections.singletonList(userId));
                            }
                        }
                    }
                }
                return result;
            } catch (Exception e) {
                LOGGER.warn(String.format("Error assigning access level %s to user %s.", accessLevel.name(), username));
                return true;
            }
        }
        return false;
    }

    private boolean teamIdsChanged(List<String> newTeamIds, List<String> orgTeamIds) {
        Set<String> oldIds = new HashSet<>(orgTeamIds);
        Set<String> newIds = new HashSet<>(newTeamIds);
        Set<String> diffIds1 = newIds.stream().filter(teamId -> !oldIds.contains(teamId)).collect(Collectors.toSet());
        Set<String> diffIds2 = oldIds.stream().filter(teamId -> !newIds.contains(teamId)).collect(Collectors.toSet());
        if (!diffIds1.isEmpty() || !diffIds2.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean assignAccessLevel(AccessLevel accessLevel, String tenantId, String username, String createdByUser,
            Long expirationDate, boolean createUser) {
        return assignAccessLevel(accessLevel, tenantId, username, createdByUser, expirationDate, createUser, false,
                null);
    }

    private boolean resignAccessLevel(String tenantId, String username, List<String> rights) {
        boolean success = true;
        for (AccessLevel accessLevel : AccessLevel.values()) {
            try {
                if (rights.contains(accessLevel.name())) {
                    success = success
                            && globalUserManagementService.revokeRight(accessLevel.name(), tenantId, username);
                }
            } catch (Exception e) {
                LOGGER.warn(
                        String.format("Error resigning access level %s from user %s.", accessLevel.name(), username));
            }
        }
        return success;
    }

    @Override
    public boolean resignAccessLevel(String tenantId, String username) {
        List<String> rights = globalUserManagementService.getRights(username, tenantId);
        return resignAccessLevel(tenantId, username, rights);
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
    public User findByEmail(String email) {
        return globalUserManagementService.getUserByEmail(email);
    }

    @Override
    public User findByUsername(String username) {
        return globalUserManagementService.getUserByUsername(username);
    }

    @Override
    public List<User> getUsers(String tenantId, UserFilter filter, List<GlobalAuthUserTenantRight> globalAuthUserTenantRights, boolean withTeam) {
        List<User> users = new ArrayList<>();
        try {
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService
                    .getAllUsersOfTenant(tenantId, globalAuthUserTenantRights, withTeam);
            for (Map.Entry<User, List<String>> userRights : userRightsList) {
                User user = userRights.getKey();
                AccessLevel accessLevel = AccessLevel.findAccessLevel(userRights.getValue());
                if (accessLevel != null) {
                    user.setAccessLevel(accessLevel.name());
                }
                LOGGER.info("access is {}, filter visible is {}", accessLevel, filter.visible(user));
                if (filter.visible(user))
                    users.add(user);
            }
        } catch (LedpException e) {
            LOGGER.warn(String.format("Trying to get all users from a non-existing tenant %s", tenantId));
            if (e.getCode() == LedpCode.LEDP_18001) {
                throw new LoginException(e);
            }
        }
        return users;
    }

    @Override
    public List<User> getUsers(String tenantId, UserFilter filter, boolean withTeam) {
        return getUsers(tenantId, filter, null, withTeam);
    }

    @Override
    public List<User> getUsers(String tenantId) {
        return getUsers(tenantId, UserFilter.TRIVIAL_FILTER, false);
    }

    @Override
    public List<User> getUsers(String tenantId, UserFilter filter) {
        return getUsers(tenantId, filter, false);
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
    public RegistrationResult registerUserWithNoTenant(UserRegistration userReg) {
        RegistrationResult result = new RegistrationResult();
        result.setValid(true);

        User user = userReg.getUser();
        user.setUsername(user.getUsername().toLowerCase());
        user.setEmail(user.getEmail().toLowerCase());

        String email = user.getEmail();
        String username = user.getUsername();

        User oldUser = findByEmail(email);
        if (oldUser != null) {
            result.setValid(false);
            result.setConflictingUser(oldUser);
        }

        if (result.isValid()) {
            oldUser = findByUsername(username);
            if (oldUser != null) {
                result.setValid(false);
                result.setConflictingUser(oldUser);
            }
        }

        if (result.isValid()) {
            boolean flag = createUser(null, userReg);
            result.setValid(flag);
        }

        if (result.isValid()) {
            String tempPass = globalUserManagementService.resetLatticeCredentials(user.getUsername());
            result.setPassword(tempPass);
        }

        return result;
    }

    @Override
    public RegistrationResult registerUserToTenant(String userName,
            UserRegistrationWithTenant userRegistrationWithTenant) {
        UserRegistration userRegistration = userRegistrationWithTenant.getUserRegistration();
        userRegistration.toLowerCase();
        User user = userRegistration.getUser();
        String tenantId = userRegistrationWithTenant.getTenant();

        RegistrationResult result = validateNewUser(user, tenantId);
        if (!result.isValid()) {
            return result;
        }

        result.setValid(createUser(userName, userRegistration));
        if (!result.isValid()) {
            return result;
        }

        if (StringUtils.isNotEmpty(user.getAccessLevel())) {
            List<String> userTeamIds = null;
            if (user.getUserTeams() != null) {
                userTeamIds = user.getUserTeams().stream().map(GlobalTeam::getTeamId).collect(Collectors.toList());
            }
            assignAccessLevel(AccessLevel.valueOf(user.getAccessLevel()), tenantId, user.getUsername(), userName,
                    user.getExpirationDate(), true, false, userTeamIds);
        }

        String tempPass = globalUserManagementService.resetLatticeCredentials(user.getUsername());
        result.setPassword(tempPass);

        return result;
    }

    @Override
    public boolean updateCredentials(User user, UserUpdateData data) {
        return updateCredentials(user, data, false);
    }

    @Override
    public boolean updateClearTextCredentials(User user, UserUpdateData data) {
        return updateCredentials(user, data, true);
    }

    /*
     * if isCredentialsClearText is true => password in UserUpdateData is clear text
     * false => password in UserUpdateData is SHA256 hashed
     */
    private boolean updateCredentials(User user, UserUpdateData data, boolean isCredentialsClearText) {
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
                if (isCredentialsClearText) {
                    oldPassword = DigestUtils.sha256Hex(oldPassword);
                }
                ticket = globalAuthenticationService.authenticateUser(user.getUsername(), oldPassword);
                if (ticket == null) {
                    return false;
                }
            } catch (LedpException e) {
                if (LedpCode.LEDP_18001.equals(e.getCode()) || LedpCode.LEDP_19015.equals(e.getCode())) {
                    return false;
                }
                throw new LoginException(e);
            }

            boolean updateSucceeded;
            if (isCredentialsClearText) {
                updateSucceeded = globalUserManagementService.modifyClearTextLatticeCredentials(ticket, oldCreds,
                        newCreds);
            } else {
                updateSucceeded = globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds);
            }
            if (updateSucceeded) {
                LOGGER.info(String.format("%s changed his/her password", user.getUsername()));
                return true;
            }
        }
        return false;
    }

    private RegistrationResult validateNewUser(User newUser, String tenantId) {
        String email = newUser.getEmail();
        User oldUser = findByEmail(email);
        if (oldUser != null) {
            if (globalUserManagementService.userExpireInTenant(oldUser.getEmail(), tenantId)) {
                deleteUser(tenantId, oldUser.getUsername());
            }
        }
        RegistrationResult result = new RegistrationResult();
        result.setValid(true);
        result.setValidEmail(true);
        if (!emailValidator.isValid(email)) {
            result.setValid(false);
            result.setValidEmail(false);
            result.setErrMsg("Not a valid email address");
            return result;
        }
        long currentTime = System.currentTimeMillis();
        if (newUser.getExpirationDate() != null && newUser.getExpirationDate() <= currentTime) {
            result.setValid(false);
            result.setErrMsg("Not a valid expire after");
            return result;
        }

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
                            success = success && globalUserManagementService.revokeRight(right.getAuthority(), tenantId,
                                    username);
                        }
                    } catch (Exception ignore) {
                        // right already revoked
                    }
                }
            }
            Long userId = findIdByUsername(username);
            clearSession(tenantId, Collections.singletonList(userId));
            return success;
        } else {
            return false;
        }
    }

    @Override
    public String deactiveUserStatus(String userName, String emails) {
        return globalUserManagementService.deactiveUserStatus(userName, emails);
    }

    @Override
    public GlobalAuthUser findByEmailNoJoin(String email) {
        return globalUserManagementService.findByEmailNoJoin(email);
    }

    @Override
    public boolean deleteUserByEmail(String email) {
        return globalUserManagementService.deleteUserByEmail(email);
    }

    @Override
    public String addUserAccessLevel(String userName, String emails, AccessLevel level) {
        LOGGER.info(String.format("%s sets user %s to %s", userName, emails, level));
        return globalUserManagementService.addUserAccessLevel(userName, emails, level);
    }

    private Long findIdByUsername(String username) {
        return globalUserManagementService.getIdByUsername(username);
    }

    private void clearSession(String tenantId, List<Long> userIds) {
        clearSession(true, tenantId, userIds);
    }

    @Override
    public void clearSession(boolean expireSession, String tenantId, List<Long> userIds) {
        if (CollectionUtils.isNotEmpty(userIds)) {
            LOGGER.info(String.format("Will clear sessions for user ids %d in %s.", userIds, tenantId));
            clearSessionService.submit(() -> {
                GlobalAuthTenant tenantData = globalTenantManagementService.findByTenantId(tenantId);
                List<GlobalAuthTicket> globalAuthTickets = globalSessionManagementService
                        .findTicketsByUserIdsAndTenant(userIds, tenantData);
                discardTickets(expireSession, globalAuthTickets, tenantData.getPid());
            });
        }
    }

    private void discardTickets(boolean expireSession, List<GlobalAuthTicket> globalAuthTickets, Long tenantId) {
        LOGGER.info(String.format("Ticket ids in %s will be deleted and expireSession value is %s.",
                globalAuthTickets.stream().map(GlobalAuthTicket::getPid).collect(Collectors.toList()), expireSession));
        for (GlobalAuthTicket globalAuthTicket : globalAuthTickets) {
            globalSessionManagementService.discardSession(expireSession, new Ticket(globalAuthTicket.getTicket()),
                    tenantId, globalAuthTicket.getPid(), globalAuthTicket.getUserId());
        }
    }
}
