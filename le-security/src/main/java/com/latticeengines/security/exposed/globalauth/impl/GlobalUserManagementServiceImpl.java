package com.latticeengines.security.exposed.globalauth.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthAuthenticationEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.EmailSettings;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.zendesk.ZendeskUser;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.globalauth.zendesk.ZendeskService;
import com.latticeengines.security.util.GlobalAuthPasswordUtils;

@Component("globalUserManagementService")
public class GlobalUserManagementServiceImpl extends GlobalAuthenticationServiceBaseImpl implements
        GlobalUserManagementService {

    private static final Logger log = LoggerFactory.getLogger(GlobalUserManagementServiceImpl.class);

    private static final String LATTICE_ENGINES_COM = "LATTICE-ENGINES.COM";

    @Value("${monitor.emailsettings.from}")
    private String EMAIL_FROM;

    @Value("${monitor.emailsettings.server}")
    private String EMAIL_SERVER;

    @Value("${monitor.emailsettings.username}")
    private String EMAIL_USERNAME;

    @Value("${monitor.emailsettings.password}")
    private String EMAIL_PASSWORD;

    @Value("${monitor.emailsettings.port}")
    private int EMAIL_PORT;

    @Value("${monitor.emailsettings.useSSL}")
    private boolean EMAIL_USESSL;

    @Value("${security.zendesk.enabled:false}")
    private boolean zendeskEnabled;

    @Autowired
    private GlobalAuthAuthenticationEntityMgr gaAuthenticationEntityMgr;

    @Autowired
    private GlobalAuthUserEntityMgr gaUserEntityMgr;

    @Autowired
    private GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Autowired
    private GlobalAuthUserTenantRightEntityMgr gaUserTenantRightEntityMgr;

    @Autowired
    private GlobalAuthTicketEntityMgr gaTicketEntityMgr;

    @Autowired
    private EmailService emailService;

    @Autowired
    private ZendeskService zendeskService;

    @Override
    public synchronized Boolean registerUser(String userName, User user, Credentials creds) {
        try {
            log.info(String.format("Registering user %s against Global Auth.", creds.getUsername()));
            return globalAuthRegisterUser(userName, user, creds);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18004, e, new String[]{creds.getUsername()});
        }
    }

    @Override
    public synchronized Boolean registerExternalIntegrationUser(String userName, User user) {
        try {
            log.info(String.format("Registering external integration user %s against Global Auth.", user.getEmail()));
            createGlobalAuthUser(userName, user, true);
            return true;
        } catch (LedpException le) {
            throw le;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18004, e, new String[]{"External: " + user.getEmail()});
        }
    }

    private Boolean globalAuthRegisterUser(String userName, User user, Credentials creds) throws Exception {
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr
                .findByUsername(creds.getUsername());
        if (latticeAuthenticationData != null) {
            throw new Exception("The specified user already exists");
        }

        latticeAuthenticationData = gaAuthenticationEntityMgr.findByUsername(user.getEmail());
        if (latticeAuthenticationData != null) {
            throw new Exception("The specified email already exists as a username.");
        }

        GlobalAuthUser userData = gaUserEntityMgr.findByEmailJoinAuthentication(user.getUsername());
        if (userData != null) {
            throw new Exception("The specified username already exists as an email.");
        }

        userData = gaUserEntityMgr.findByEmailJoinAuthentication(user.getEmail());
        if (userData != null) {
            throw new Exception("The specified email already exists.");
        }

        userData = createGlobalAuthUser(userName, user, false);

        latticeAuthenticationData = new GlobalAuthAuthentication();
        latticeAuthenticationData.setUsername(creds.getUsername());
        latticeAuthenticationData.setPassword(creds.getPassword());
        latticeAuthenticationData.setGlobalAuthUser(userData);
        Date now = new Date(System.currentTimeMillis());
        latticeAuthenticationData.setCreationDate(now);
        latticeAuthenticationData.setLastModificationDate(now);
        gaAuthenticationEntityMgr.create(latticeAuthenticationData);
        return true;
    }

    protected GlobalAuthUser createGlobalAuthUser(String userName, User user, boolean externalIntegUser) {
        if (externalIntegUser && StringUtils.isNotBlank(user.getEmail()) && user.getEmail().toUpperCase().endsWith(LATTICE_ENGINES_COM)) {
            throw new LedpException(LedpCode.LEDP_19004);
        }
        GlobalAuthUser userData;
        userData = new GlobalAuthUser();
        userData.setEmail(user.getEmail());
        userData.setFirstName(user.getFirstName());
        userData.setLastName(user.getLastName());
        userData.setTitle(user.getTitle());
        userData.setPhoneNumber(user.getPhoneNumber());
        userData.setIsActive(true);
        userData.setCreatedByUser(userName);
        gaUserEntityMgr.create(userData);
        return userData;
    }

    @Override
    public Boolean grantRight(String right, String tenant, String username) {
        try {
            log.info(String.format("Granting right %s to user %s for tenant %s.", right, username,
                    tenant));
            return globalAuthGrantRight(right, tenant, username, null, null);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18005, e,
                    new String[]{right, username, tenant});
        }
    }

    @Override
    public synchronized Boolean grantRight(String right, String tenant, String username, String createdByUser,
                                           Long expirationDate) {
        try {
            log.info(String.format("Granting right %s to user %s for tenant %s with expiration period %s.", right,
                    username, tenant, expirationDate));
            return globalAuthGrantRight(right, tenant, username, createdByUser, expirationDate);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18005, e,
                    new String[]{right, username, tenant});
        }
    }

    public Boolean globalAuthGrantRight(String right, String tenant, String username, String createdByUser,
                                        Long expirationDate)
            throws Exception {

        GlobalAuthUser globalAuthUser = findGlobalAuthUserByUsername(username, true);

        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant);
        if (tenantData == null) {
            throw new Exception("Unable to find the tenant requested.");
        }

        GlobalAuthUserTenantRight rightData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantIdAndOperationName(
                        globalAuthUser.getPid(),
                        tenantData.getPid(), right);
        if (rightData != null) {
            // update expiration date of tenant for user
            // if expiration date changes, update tenant right
            if (expireDateChanged(rightData.getExpirationDate(), expirationDate)) {
                rightData.setExpirationDate(expirationDate);
                gaUserTenantRightEntityMgr.update(rightData);
            }
            return true;
        }

        rightData = new GlobalAuthUserTenantRight();
        rightData.setGlobalAuthUser(globalAuthUser);
        rightData.setGlobalAuthTenant(tenantData);
        rightData.setOperationName(right);
        rightData.setCreatedByUser(createdByUser);
        rightData.setExpirationDate(expirationDate);
        gaUserTenantRightEntityMgr.create(rightData);

        if (isZendeskEnabled(globalAuthUser.getEmail())) {
            List<GlobalAuthUserTenantRight> rights = gaUserTenantRightEntityMgr.findByEmail(globalAuthUser.getEmail());
            if (rights.size() == 1) {
                // originally no rights, activate zendesk user
                log.info(String.format("Activating zendesk user %s.", globalAuthUser.getEmail()));
                zendeskService.unsuspendUserByEmail(globalAuthUser.getEmail());
            }
        }
        return true;
    }

    protected GlobalAuthUser findGlobalAuthUserByUsername(String username) throws Exception {
        return findGlobalAuthUserByUsername(username, false);
    }

    protected GlobalAuthUser findGlobalAuthUserByUsername(String username, boolean assertUserExistence) throws Exception {
        GlobalAuthUser globalAuthUser = findByEmailNoJoin(username);
        if (globalAuthUser != null) {
            return globalAuthUser;
        }
        // This is the corner case for admin User. Other than that, for all other users username and email are same.
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr
                .findByUsernameJoinUser(username);
        if (latticeAuthenticationData != null) {
            globalAuthUser = latticeAuthenticationData.getGlobalAuthUser();
            return globalAuthUser;
        }
        if (assertUserExistence) {
            throw new Exception("Unable to find the user requested.");
        }
        return null;
    }

    @Override
    public List<String> getRights(String username, String tenantId) {
        try {
            log.info(String.format("Getting rights of user %s in tenant %s.", username, tenantId));
            return globalAuthGetRights(tenantId, username);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("Sequence contains no elements")) {
                return new ArrayList<>();
            }
            throw new LedpException(LedpCode.LEDP_18000,
                    "Getting rights of user " + username + " in tenant " + tenantId + ".", e);
        }
    }

    private List<GlobalAuthUserTenantRight> globalAuthGetRightsDetail(String email, String tenantId) {
        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenantId);
        if (tenantData == null) {
            return new ArrayList<>();
        }
        GlobalAuthUser userData = gaUserEntityMgr.findByEmailJoinAuthentication(email);
        if (userData == null) {
            return new ArrayList<>();
        }
        List<GlobalAuthUserTenantRight> rightsData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantId(userData.getPid(),
                        tenantData.getPid());
        if (rightsData != null) {
            return rightsData;
        } else {
            return new ArrayList<>();
        }

    }

    private List<String> globalAuthGetRights(String tenantId, String username) throws Exception {
        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenantId);
        if (tenantData == null) {
            return new ArrayList<>();
        }

        GlobalAuthUser globalAuthUser = findGlobalAuthUserByUsername(username);
        if (globalAuthUser == null) {
            return new ArrayList<>();
        }

        List<GlobalAuthUserTenantRight> rightsData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantId(globalAuthUser.getPid(),
                        tenantData.getPid());
        if (rightsData != null) {

            Set<String> distinctRights = new HashSet<String>();
            for (GlobalAuthUserTenantRight rightData : rightsData) {
                distinctRights.add(rightData.getOperationName());
            }
            List<String> rights = new ArrayList<String>(distinctRights);
            return rights;
        } else {
            return new ArrayList<>();
        }

    }

    @Override
    public synchronized Boolean revokeRight(String right, String tenant, String username) {
        try {
            log.info(String.format("Revoking right %s from user %s for tenant %s.", right,
                    username, tenant));
            return globalAuthRevokeRight(right, tenant, username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18006, e,
                    new String[]{right, username, tenant});
        }
    }

    private Boolean globalAuthRevokeRight(String right, String tenant, String username)
            throws Exception {
        GlobalAuthUser globalAuthUser = findGlobalAuthUserByUsername(username, true);

        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant);
        if (tenantData == null) {
            throw new Exception("Unable to find the tenant requested.");
        }

        GlobalAuthUserTenantRight rightData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantIdAndOperationName(
                        globalAuthUser.getPid(),
                        tenantData.getPid(), right);
        if (rightData == null) {
            return true;
        }

        gaUserTenantRightEntityMgr.delete(rightData);

        if (isZendeskEnabled(globalAuthUser.getEmail())) {
            List<GlobalAuthUserTenantRight> rights = gaUserTenantRightEntityMgr.findByEmail(globalAuthUser.getEmail());
            if (rights.isEmpty()) {
                // no rights, deactive zendesk user
                log.info(String.format("Deactivating zendesk user %s.", globalAuthUser.getEmail()));
                zendeskService.suspendUserByEmail(globalAuthUser.getEmail());
            }
        }
        return true;
    }

    @Override
    public synchronized Boolean forgotLatticeCredentials(String username) {

        if (getUserByUsername(username) == null) {
            throw new LedpException(LedpCode.LEDP_18018, new String[]{username});
        }

        EmailSettings emailsettings = new EmailSettings();
        emailsettings.setFrom(EMAIL_FROM);
        emailsettings.setServer(EMAIL_SERVER);
        emailsettings.setUsername(EMAIL_USERNAME);
        emailsettings.setPassword(EMAIL_PASSWORD);
        emailsettings.setPort(EMAIL_PORT);
        emailsettings.setUseSSL(EMAIL_USESSL);

        try {
            log.info(String.format("Resetting credentials for user %s.", username));
            return globalAuthForgotLatticeCredentials(username, emailsettings);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18011, e, new String[]{username});
        }
    }

    private Boolean globalAuthForgotLatticeCredentials(String username, EmailSettings emailsettings)
            throws Exception {
        // For forgot password we should check for User Existence in "GlobalAuthentication" instead of "GlobalUser"
        // Because, for SAML auto provisioned users, we should not allow them to change / create password in GlobalAuth system.
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr
                .findByUsername(username);
        if (latticeAuthenticationData == null) {
            throw new Exception("Unable to find the user requested.");
        }

        GlobalAuthUser userData = gaUserEntityMgr
                .findByUserIdWithTenantRightsAndAuthentications(latticeAuthenticationData
                        .getGlobalAuthUser().getPid());
        if (userData == null) {
            throw new Exception("Unable to find the user requested.");
        }

        if (!userData.getIsActive()) {
            throw new Exception("The user is inactive!");
        }
        String userEmail = userData.getEmail();
        if (userEmail == null || userEmail.isEmpty() || userEmail.trim().isEmpty()) {
            throw new Exception("The specified user does not have an email address specified: "
                    + userData.getPid().toString());
        }

        String password = GlobalAuthPasswordUtils.getSecureRandomString(16);
        latticeAuthenticationData.setPassword(GlobalAuthPasswordUtils
                .encryptPassword(GlobalAuthPasswordUtils.hash256(password)));
        latticeAuthenticationData.setMustChangePassword(true);
        gaAuthenticationEntityMgr.update(latticeAuthenticationData);

        if (isZendeskEnabled(userData.getEmail()) && userData.getUserTenantRights() != null &&
                !userData.getUserTenantRights().isEmpty()) {
            // user has tenant rights, make sure zendesk account exists and set password
            ZendeskUser zendeskUser = upsertZendeskUser(userData);
            log.info(String.format("Resetting credentials for zendesk user %s.", userData.getEmail()));
            zendeskService.setUserPassword(zendeskUser.getId(), password);
        }

        emailService.sendGlobalAuthForgetCredsEmail(userData.getFirstName(),
                userData.getLastName(), userData.getAuthentications().get(0)
                        .getUsername(), password, userData.getEmail(), emailsettings);
        return true;
    }

    @Override
    public synchronized Boolean modifyLatticeCredentials(Ticket ticket, Credentials oldCreds,
                                                         Credentials newCreds) {
        try {
            log.info(String.format("Modifying credentials for %s.", oldCreds.getUsername()));
            return globalAuthModifyLatticeCredentials(ticket, oldCreds, newCreds, null);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18010, e, new String[]{oldCreds.getUsername()});
        }
    }

    @Override
    public Boolean modifyClearTextLatticeCredentials(Ticket ticket, Credentials oldCreds, Credentials newCreds) {
        try {
            String password = newCreds.getPassword();
            oldCreds.setPassword(DigestUtils.sha256Hex(oldCreds.getPassword()));
            newCreds.setPassword(DigestUtils.sha256Hex(newCreds.getPassword()));
            log.info(String.format("Modifying credentials for %s.", oldCreds.getUsername()));
            return globalAuthModifyLatticeCredentials(ticket, oldCreds, newCreds, (userData) -> {
                if (!isZendeskEnabled(userData.getEmail())) {
                    return;
                }
                if (userData.getUserTenantRights() != null && !userData.getUserTenantRights().isEmpty()) {
                    // user has tenant rights, make sure zendesk account exists and set password
                    ZendeskUser zendeskUser = upsertZendeskUser(userData);
                    log.info(String.format("Modifying credentials for zendesk user %s.", userData.getEmail()));
                    zendeskService.setUserPassword(zendeskUser.getId(), password);
                }
            });
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18010, e, new String[]{oldCreds.getUsername()});
        }
    }

    private Boolean globalAuthModifyLatticeCredentials(
            Ticket ticket, Credentials oldCreds, Credentials newCreds, Consumer<GlobalAuthUser> callback) throws Exception {
        GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());
        if (ticketData == null) {
            throw new Exception("Unable to find the ticket requested.");
        }
        GlobalAuthUser userData = gaUserEntityMgr.findByUserIdWithTenantRightsAndAuthentications(ticketData.getUserId());
        GlobalAuthAuthentication latticeAuthenticationData = userData.getAuthentications().get(0);
        if (latticeAuthenticationData == null) {
            throw new Exception("Unable to find the credentials requested.");
        }
        if (!latticeAuthenticationData.getPassword().equals(GlobalAuthPasswordUtils
                .encryptPassword(oldCreds.getPassword()))) {
            throw new Exception("Old password is incorrect for password change.");
        }
        if (oldCreds.getPassword().equals(newCreds.getPassword())) {
            throw new Exception("The new password cannot be the same as the old password.");
        }
        latticeAuthenticationData.setPassword(GlobalAuthPasswordUtils.encryptPassword(newCreds
                .getPassword()));
        latticeAuthenticationData.setMustChangePassword(false);
        gaAuthenticationEntityMgr.update(latticeAuthenticationData);

        if (callback != null) {
            callback.accept(userData);
        }
        return true;
    }

    @Override
    public synchronized String resetLatticeCredentials(String username) {
        try {
            log.info(String.format("Resetting credentials for %s.", username));
            return globalAuthResetLatticeCredentials(username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18011, e, new String[]{username});
        }
    }

    private String globalAuthResetLatticeCredentials(String username) throws Exception {
        String password = "";
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr
                .findByUsername(username);
        if (latticeAuthenticationData == null) {
            throw new Exception("Unable to find the user requested.");
        }

        GlobalAuthUser userData = latticeAuthenticationData.getGlobalAuthUser();
        if (userData == null) {
            throw new Exception("Unable to find the user requested.");
        }
        if (!userData.getIsActive()) {
            throw new Exception("The user is inactive!");
        }
        String userEmail = userData.getEmail();
        if (userEmail == null || userEmail.isEmpty() || userEmail.trim().isEmpty()) {
            throw new Exception("The specified user does not have an email address specified: "
                    + userData.getPid().toString());
        }
        password = GlobalAuthPasswordUtils.getSecureRandomString(16);
        latticeAuthenticationData.setPassword(GlobalAuthPasswordUtils
                .encryptPassword(GlobalAuthPasswordUtils.hash256(password)));
        latticeAuthenticationData.setMustChangePassword(true);
        gaAuthenticationEntityMgr.update(latticeAuthenticationData);
        userData.setInvalidLoginAttempts(0);
        gaUserEntityMgr.update(userData);

        if (isZendeskEnabled(userData.getEmail())) {
            List<GlobalAuthUserTenantRight> rights = gaUserTenantRightEntityMgr.findByEmail(userData.getEmail());
            if (!rights.isEmpty()) {
                // user has tenant rights, make sure zendesk account exists and set password
                ZendeskUser zendeskUser = upsertZendeskUser(userData);
                log.info(String.format("Resetting credentials for zendesk user %s.", userData.getEmail()));
                zendeskService.setUserPassword(zendeskUser.getId(), password);
            }
        }

        return password;
    }

    @Override
    public User getUserByEmail(String email) {
        try {
            log.info(String.format("Getting user having the email %s.", email));
            return globalAuthFindUserByEmail(email);
        } catch (Exception e) {
            // TODO: handle different exceptions returned from GlobalAuth
            return null;
        }
    }

    private User globalAuthFindUserByEmail(String email) throws Exception {
        User user = new User();
        GlobalAuthUser userData = gaUserEntityMgr.findByEmailJoinAuthentication(email);
        if (userData == null) {
            throw new Exception("Unable to find the user requested.");
        }
        GlobalAuthAuthentication authData = null;
        if (userData.getAuthentications() != null && userData.getAuthentications().size() > 0) {
            authData = userData.getAuthentications().get(0);
        }
        if (authData != null) {
            user.setUsername(authData.getUsername());
        }
        if (userData.getEmail() != null) {
            user.setEmail(userData.getEmail());
        }

        if (userData.getFirstName() != null) {
            user.setFirstName(userData.getFirstName());
        }

        if (userData.getLastName() != null) {
            user.setLastName(userData.getLastName());
        }

        if (userData.getTitle() != null) {
            user.setTitle(userData.getTitle());
        }

        if (userData.getPhoneNumber() != null) {
            user.setPhoneNumber(userData.getPhoneNumber());
        }

        if (userData.getIsActive()) {
            user.setActive(userData.getIsActive());
        }
        return user;
    }

    @Override
    public User getUserByUsername(String username) {
        try {
            log.info(String.format("Getting user %s.", username));
            return globalAuthFindUserByUsername(username);
        } catch (Exception e) {
            // TODO: handle different exceptions returned from GlobalAuth
            return null;
        }
    }

    private User globalAuthFindUserByUsername(String username) throws Exception {
        User user = new User();

        GlobalAuthUser userData = findGlobalAuthUserByUsername(username, true);

        user.setUsername(username);
        if (userData.getEmail() != null) {
            user.setEmail(userData.getEmail());
        }

        if (userData.getFirstName() != null) {
            user.setFirstName(userData.getFirstName());
        }

        if (userData.getLastName() != null) {
            user.setLastName(userData.getLastName());
        }

        if (userData.getTitle() != null) {
            user.setTitle(userData.getTitle());
        }

        if (userData.getPhoneNumber() != null) {
            user.setPhoneNumber(userData.getPhoneNumber());
        }

        if (userData.getIsActive()) {
            user.setActive(userData.getIsActive());
        }

        return user;
    }

    @Override
    public synchronized Boolean deleteUser(String username) {
        try {
            boolean result = globalAuthDeleteUser(username);
            log.info(String.format("Deleting user %s success = %s", username, result));
            return result;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18015, e, new String[]{username});
        }
    }

    private Boolean globalAuthDeleteUser(String username) throws Exception {
        GlobalAuthUser userData = findGlobalAuthUserByUsername(username);
        if (userData == null) {
            return true;
        }

        try {
            gaUserEntityMgr.delete(userData);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw new Exception("Unable to delete the user requested.");
        }

        // disable zendesk user
        if (isZendeskEnabled(userData.getEmail())) {
            log.info(String.format("Deactivating zendesk user %s.", userData.getEmail()));
            zendeskService.suspendUserByEmail(userData.getEmail());
        }
        return true;
    }

    @Override
    public Boolean isRedundant(String username) {
        try {
            boolean isRedundant = gaUserTenantRightEntityMgr.isRedundant(username);
            if (isRedundant) {
                log.info(String.format("User %s is redundant.", username));
            } else {
                log.info("User " + username + " is not redundant, it is still used by some tenants");
            }
            return isRedundant;
        } catch (Exception e) {
            log.error("Failed to check if the user " + username + " is redundant.", e);
            return false;
        }
    }

    @Override
    public void checkRedundant(String username) {
        if (isZendeskEnabled(username) && isRedundant(username)) {
            // disable zendesk user
            log.info(String.format("Deactivating zendesk user %s.", username));
            zendeskService.suspendUserByEmail(username);
        }
    }

    @Override
    public List<AbstractMap.SimpleEntry<User, List<String>>> getAllUsersOfTenant(String tenantId) {
        try {
            log.info(String.format("Getting all users and their rights for tenant %s.", tenantId));
            return globalFindAllUserRightsByTenant(tenantId);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18016, e, new String[]{tenantId});
        }
    }

    private List<AbstractMap.SimpleEntry<User, List<String>>> globalFindAllUserRightsByTenant(
            String tenantId) throws Exception {
        List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = new ArrayList<>();
        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenantId);
        if (tenantData == null) {
            throw new Exception("Unable to find the tenant requested: " + tenantId);
        }
        List<GlobalAuthUserTenantRight> userRightDatas = gaUserTenantRightEntityMgr.findByTenantId(tenantData.getPid());
        if (userRightDatas == null || userRightDatas.size() == 0) {
            return userRightsList;
        }

        HashMap<Long, String> userIdToUsername = gaUserEntityMgr.findUserInfoByTenant(tenantData);

        HashMap<Long, AbstractMap.SimpleEntry<User, HashSet<String>>> userRights = new HashMap<>();
        for (GlobalAuthUserTenantRight userRightData : userRightDatas) {
            GlobalAuthUser userData = userRightData.getGlobalAuthUser();
            if (userRights.containsKey(userData.getPid())) {
                AbstractMap.SimpleEntry<User, HashSet<String>> uRights = userRights.get(userData.getPid());
                uRights.getValue().add(userRightData.getOperationName());
            } else {
                User user = new User();
                user.setUsername(userIdToUsername.get(userData.getPid()));

                if (userData.getEmail() != null) {
                    user.setEmail(userData.getEmail());
                }

                if (userData.getFirstName() != null) {
                    user.setFirstName(userData.getFirstName());
                }

                if (userData.getLastName() != null) {
                    user.setLastName(userData.getLastName());
                }

                if (userData.getTitle() != null) {
                    user.setTitle(userData.getTitle());
                }

                if (userData.getPhoneNumber() != null) {
                    user.setPhoneNumber(userData.getPhoneNumber());
                }

                if (userData.getIsActive()) {
                    user.setActive(userData.getIsActive());
                }

                if (StringUtils.isNotEmpty(userRightData.getOperationName())) {
                    user.setAccessLevel(userRightData.getOperationName());
                }

                if (userRightData.getExpirationDate() != null) {
                    user.setExpirationDate(userRightData.getExpirationDate());
                }

                AbstractMap.SimpleEntry<User, HashSet<String>> uRights = new AbstractMap.SimpleEntry<>(user,
                        new HashSet<String>());
                uRights.getValue().add(userRightData.getOperationName());
                userRights.put(userData.getPid(), uRights);
            }
        }
        for (Map.Entry<Long, AbstractMap.SimpleEntry<User, HashSet<String>>> entry : userRights.entrySet()) {
            List<String> rights = new ArrayList<>(entry.getValue().getValue());
            AbstractMap.SimpleEntry<User, List<String>> uRights = new AbstractMap.SimpleEntry<>(
                    entry.getValue().getKey(), rights);
            userRightsList.add(uRights);
        }
        return userRightsList;
    }

    @Override
    public String deactiveUserStatus(String userName, String emails) {
        String[] emailStr = emails.trim().split(",");
        Set<String> emailSet = new HashSet<>();
        for (String email : emailStr) {
            emailSet.add(email.trim());
        }
        StringBuilder filterEmails = new StringBuilder("");
        for (String email : emailSet) {
            GlobalAuthUser gaUser = gaUserEntityMgr.findByEmail(email);
            if (gaUser == null) {
                log.info(String.format("the email %s is not valid, and can't find user in table GlobalUser", email));
                continue;
            }
            filterEmails.append(email + ",");
            gaUser.setIsActive(false);
            gaUserEntityMgr.update(gaUser);
            log.info(String.format("%s set user %s isActive to false", userName, gaUser.getEmail()));
            gaUserTenantRightEntityMgr.deleteByUserId(gaUser.getPid());
            log.info(String.format("%s delete the %s's GlobalUserTenantRight", userName, gaUser.getEmail()));

            // disable zendesk user
            if (isZendeskEnabled(email)) {
                zendeskService.suspendUserByEmail(email);
            }
        }
        return filterEmails.toString();
    }

    @Override
    public GlobalAuthUser findByEmailNoJoin(String email) {
        return gaUserEntityMgr.findByEmail(email);
    }

    @Override
    public boolean deleteUserByEmail(String email) {
        GlobalAuthUser gaUser = gaUserEntityMgr.findByEmail(email);
        if (gaUser == null)
            return true;

        GlobalAuthAuthentication gaAuthentication = gaAuthenticationEntityMgr.findByUserId(gaUser.getPid());
        if (gaAuthentication != null) {
            gaAuthenticationEntityMgr.delete(gaAuthentication);
            log.info(String.format("current user's auth id %s is deleted", gaAuthentication.getPid()));
        }

        try {
            gaUserEntityMgr.delete(gaUser);
            log.info(String.format("current user %s is deleted", gaUser.getFirstName()));
        } catch (Exception e) {
            throw new RuntimeException("Unable to delete the user requested.");
        }

        // disable zendesk user
        if (isZendeskEnabled(email)) {
            zendeskService.suspendUserByEmail(email);
        }
        return true;
    }

    @Override
    public String addUserAccessLevel(String userName, String emails, AccessLevel level) {
        String[] emailStr = emails.trim().split(",");
        Set<String> emailSet = new HashSet<>();
        for (String email : emailStr) {
            emailSet.add(email.trim());
        }
        StringBuilder filterEmails = new StringBuilder("");
        for (String email : emailSet) {
            GlobalAuthUser gaUser = gaUserEntityMgr.findByEmail(email);
            if (gaUser == null) {
                log.info(String.format("the email %s is not valid, and can't find user in table GlobalUser", email));
                continue;
            }
            List<GlobalAuthTenant> tenants = gaTenantEntityMgr.findTenantNotInTenantRight(gaUser);
            if (tenants != null) {
                filterEmails.append(email + ",");
            }
            for (GlobalAuthTenant tenant : tenants) {
                GlobalAuthUserTenantRight gaUserTenantRight = new GlobalAuthUserTenantRight();
                gaUserTenantRight.setGlobalAuthTenant(tenant);
                gaUserTenantRight.setGlobalAuthUser(gaUser);
                gaUserTenantRight.setOperationName(level.toString());
                gaUserTenantRight.setCreatedByUser(userName);
                log.info(String.format("user %s is granted to %s", email, level));
                GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant.getId());
                if (tenantData == null) {
                    log.error(String.format("Cannot find tenant %s to grant access", tenant.getId()));
                } else {
                    try {
                        gaUserTenantRightEntityMgr.create(gaUserTenantRight);
                    } catch (Exception e) {
                        log.error(String.format("Cannot add user %s with access level %s for tenant %s", email,
                                level.toString(), tenant.getId()), e);
                    }
                }
            }

            // active zendesk user
            if (isZendeskEnabled(email)) {
                log.info(String.format("Activating zendesk user %s.", email));
                zendeskService.unsuspendUserByEmail(email);
            }
        }
        return filterEmails.toString();
    }

    @Override
    public boolean userExpireIntenant(String email, String tenantId) {
        log.info(String.format("Check  user expire in this tenant %s with email %s.", tenantId, email));
        List<GlobalAuthUserTenantRight> globalAuthUserTenantRights = globalAuthGetRightsDetail(email, tenantId);
        long currentTime = System.currentTimeMillis();
        for (GlobalAuthUserTenantRight globalAuthUserTenantRight : globalAuthUserTenantRights) {
            if (globalAuthUserTenantRight.getExpirationDate() != null && currentTime >= globalAuthUserTenantRight.getExpirationDate()) {
                return true;
            }
        }
        return false;
    }

    private boolean isZendeskEnabled(String email) {
        if (!zendeskEnabled || email == null) {
            return false;
        }
        // disable zendesk feature for Lattice email
        return !email.trim().toUpperCase().endsWith(LATTICE_ENGINES_COM);
    }

    /**
     * Make sure a zendesk user with specified email exists and update its name/verified/suspended properties
     *
     * @param user target zendesk user info
     * @return entire zendesk user after the operation
     */
    private ZendeskUser upsertZendeskUser(GlobalAuthUser user) {
        ZendeskUser req = new ZendeskUser();
        req.setEmail(user.getEmail());
        req.setName(getZendeskName(user));
        // verified and unsuspended
        req.setVerified(true);
        req.setSuspended(false);
        return zendeskService.createOrUpdateUser(req);
    }

    /*
     * get formated username for zendesk user
     */
    private String getZendeskName(@NotNull GlobalAuthUser user) {
        String firstName = Optional.ofNullable(user.getFirstName()).orElse("");
        String lastName = Optional.ofNullable(user.getLastName()).orElse("");
        return String.format("%s %s", firstName, lastName);
    }

    private boolean expireDateChanged(Long before, Long current) {
        boolean expirationDateChange = false;
        if (current == null) {
            if (before != null) {
                expirationDateChange = true;
            }
        } else {
            expirationDateChange = !current.equals(before);
        }
        return expirationDateChange;
    }

    @Override
    public Boolean existExpireDateChanged(String username, String tenant, String right, Long expirationDate) {
        GlobalAuthUser globalAuthUser = null;
        try {
            globalAuthUser = findGlobalAuthUserByUsername(username, true);
            GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant);
            if (tenantData != null) {
                GlobalAuthUserTenantRight rightData = gaUserTenantRightEntityMgr
                        .findByUserIdAndTenantIdAndOperationName(
                                globalAuthUser.getPid(),
                                tenantData.getPid(), right);
                Long before = rightData == null ? null : rightData.getExpirationDate();
                return expireDateChanged(before, expirationDate);
            }
        } catch (Exception e) {
            log.error("Unexpected exception: ", e);
        }
        return false;
    }
}
