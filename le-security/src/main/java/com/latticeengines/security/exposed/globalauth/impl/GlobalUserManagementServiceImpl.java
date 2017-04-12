package com.latticeengines.security.exposed.globalauth.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthAuthenticationEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
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
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.util.GlobalAuthPasswordUtils;

@Component("globalUserManagementService")
public class GlobalUserManagementServiceImpl extends GlobalAuthenticationServiceBaseImpl implements
        GlobalUserManagementService {

    private static final Log log = LogFactory.getLog(GlobalUserManagementServiceImpl.class);

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

    @Override
    public synchronized Boolean registerUser(User user, Credentials creds) {
        try {
            log.info(String.format("Registering user %s against Global Auth.", creds.getUsername()));
            return globalAuthRegisterUser(user, creds);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18004, e, new String[] { creds.getUsername() });
        }
    }

    private Boolean globalAuthRegisterUser(User user, Credentials creds) throws Exception {
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

        userData = new GlobalAuthUser();
        userData.setEmail(user.getEmail());
        userData.setFirstName(user.getFirstName());
        userData.setLastName(user.getLastName());
        userData.setTitle(user.getTitle());
        userData.setPhoneNumber(user.getPhoneNumber());
        userData.setIsActive(true);
        gaUserEntityMgr.create(userData);

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

    @Override
    public synchronized Boolean grantRight(String right, String tenant, String username) {
        try {
            log.info(String.format("Granting right %s to user %s for tenant %s.", right, username,
                    tenant));
            return globalAuthGrantRight(right, tenant, username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18005, e,
                    new String[] { right, username, tenant });
        }
    }

    public Boolean globalAuthGrantRight(String right, String tenant, String username)
            throws Exception {
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr
                .findByUsername(username);
        if (latticeAuthenticationData == null) {
            throw new Exception("Unable to find the user requested.");
        }

        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant);
        if (tenantData == null) {
            throw new Exception("Unable to find the tenant requested.");
        }

        GlobalAuthUserTenantRight rightData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantIdAndOperationName(
                        latticeAuthenticationData.getGlobalAuthUser().getPid(),
                        tenantData.getPid(), right);
        if (rightData != null) {
            return true;
        }

        rightData = new GlobalAuthUserTenantRight();
        rightData.setGlobalAuthUser(latticeAuthenticationData.getGlobalAuthUser());
        rightData.setGlobalAuthTenant(tenantData);
        rightData.setOperationName(right);
        gaUserTenantRightEntityMgr.create(rightData);
        return true;
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

    private List<String> globalAuthGetRights(String tenantId, String username) {
        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenantId);
        if (tenantData == null) {
            return new ArrayList<>();
        }

        GlobalAuthAuthentication authenticationData = gaAuthenticationEntityMgr
                .findByUsernameJoinUser(username);
        if (authenticationData == null) {
            return new ArrayList<>();
        }

        List<GlobalAuthUserTenantRight> rightsData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantId(authenticationData.getGlobalAuthUser().getPid(),
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
                    new String[] { right, username, tenant });
        }
    }

    private Boolean globalAuthRevokeRight(String right, String tenant, String username)
            throws Exception {
        GlobalAuthAuthentication latticeAuthenticationData = gaAuthenticationEntityMgr
                .findByUsername(username);
        if (latticeAuthenticationData == null) {
            throw new Exception("Unable to find the user requested.");
        }

        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenant);
        if (tenantData == null) {
            throw new Exception("Unable to find the tenant requested.");
        }

        GlobalAuthUserTenantRight rightData = gaUserTenantRightEntityMgr
                .findByUserIdAndTenantIdAndOperationName(
                        latticeAuthenticationData.getGlobalAuthUser().getPid(),
                        tenantData.getPid(), right);
        if (rightData == null) {
            return true;
        }

        gaUserTenantRightEntityMgr.delete(rightData);
        return true;
    }

    @Override
    public synchronized Boolean forgotLatticeCredentials(String username) {

        if (getUserByUsername(username) == null) {
            throw new LedpException(LedpCode.LEDP_18018, new String[] { username });
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
            throw new LedpException(LedpCode.LEDP_18011, e, new String[] { username });
        }
    }

    private Boolean globalAuthForgotLatticeCredentials(String username, EmailSettings emailsettings)
            throws Exception {
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
            return globalAuthModifyLatticeCredentials(ticket, oldCreds, newCreds);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18010, e, new String[] { oldCreds.getUsername() });
        }
    }

    private Boolean globalAuthModifyLatticeCredentials(Ticket ticket, Credentials oldCreds,
            Credentials newCreds) throws Exception {
        GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());
        if (ticketData == null) {
            throw new Exception("Unable to find the ticket requested.");
        }
        GlobalAuthAuthentication latticeAuthenticationData = gaUserEntityMgr
                .findByUserIdWithTenantRightsAndAuthentications(ticketData.getUserId())
                .getAuthentications().get(0);
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
        return true;
    }

    @Override
    public synchronized String resetLatticeCredentials(String username) {
        try {
            log.info(String.format("Resetting credentials for %s.", username));
            return globalAuthResetLatticeCredentials(username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18011, e, new String[] { username });
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
        GlobalAuthAuthentication authData = userData.getAuthentications().get(0);

        user.setUsername(authData.getUsername());
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
        GlobalAuthUser userData = gaAuthenticationEntityMgr.findByUsernameJoinUser(username)
                .getGlobalAuthUser();
        if (userData == null) {
            throw new Exception("Unable to find the user requested.");
        }
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
            throw new LedpException(LedpCode.LEDP_18015, e, new String[] { username });
        }
    }

    private Boolean globalAuthDeleteUser(String username) throws Exception {
        GlobalAuthAuthentication authenticationData = gaAuthenticationEntityMgr
                .findByUsernameJoinUser(username);
        if (authenticationData == null) {
            return true;
        }
        GlobalAuthUser userData = authenticationData.getGlobalAuthUser();
        if (userData == null) {
            return true;
        }
        try {
            gaUserEntityMgr.delete(userData);
        } catch (Exception e) {
            throw new Exception("Unable to delete the user requested.");
        }
        return true;
    }

    @Override
    public Boolean isRedundant(String username) {
        try {
            List<GlobalAuthUserTenantRight> rights = gaUserTenantRightEntityMgr.findByEmail(username);
            boolean isRedundant = rights.isEmpty();
            if (isRedundant) {
                log.info(String.format("User %s is redundant.", username));
            } else {
                Set<String> tenantIds = new HashSet<>();
                for (GlobalAuthUserTenantRight right: rights) {
                    tenantIds.add(right.getGlobalAuthTenant().getId());
                }
                log.info("User " + username + " is not redundant, it is still used by " + tenantIds.size() + " tenants");
            }
            return isRedundant;
        } catch (Exception e) {
            log.error("Failed to check if the user " + username + " is redundant.", e);
            return false;
        }
    }

    @Override
    public List<AbstractMap.SimpleEntry<User, List<String>>> getAllUsersOfTenant(String tenantId) {
        try {
            log.info(String.format("Getting all users and their rights for tenant %s.", tenantId));
            return globalFindAllUserRightsByTenant(tenantId);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18016, e, new String[] { tenantId });
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

        HashMap<Long, String> userIdToUsername = gaAuthenticationEntityMgr.findUserInfoByTenantId(tenantData.getPid());

        HashMap<Long, AbstractMap.SimpleEntry<User, HashSet<String>>> userRights = new HashMap<>();
        for(GlobalAuthUserTenantRight userRightData : userRightDatas) {
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
    public Boolean deactiveUserStatus(String userName, String emails) {
        String[] emailStr = emails.trim().split(",");
        for (String email : emailStr) {
            GlobalAuthUser gaUser = gaUserEntityMgr.findByEmail(email.trim());
            if (gaUser != null) {
                gaUser.setIsActive(false);
                gaUserEntityMgr.update(gaUser);
                log.info(String.format("%s set user %s isActive to false", userName, gaUser.getFirstName()));
                gaAuthenticationEntityMgr.deleteByUserId(gaUser.getPid());
                log.info(String.format("%s delete the %s's GlobalAuthentication", userName, gaUser.getFirstName()));
                gaUserTenantRightEntityMgr.deleteByUserId(gaUser.getPid());
                log.info(String.format("%s delete the %s's GlobalUserTenantRight", userName, gaUser.getFirstName()));
            }
        }
        return true;
    }

    @Override
    public GlobalAuthUser findByEmailNoJoin(String email) {
        return gaUserEntityMgr.findByEmail(email);
    }

    @Override
    public boolean deleteUserByEmail(String email){
        GlobalAuthUser gaUser = gaUserEntityMgr.findByEmail(email);
        if(gaUser == null)
            return true;

        GlobalAuthAuthentication gaAuthentication = gaAuthenticationEntityMgr.findByUserId(gaUser.getPid());
        if(gaAuthentication != null)
        {
            gaAuthenticationEntityMgr.delete(gaAuthentication);
            log.info(String.format("current user's auth id %s is deleted", gaAuthentication.getPid()));
        }

        try {
            gaUserEntityMgr.delete(gaUser);
            log.info(String.format("current user %s is deleted", gaUser.getFirstName()));
        } catch (Exception e) {
            throw new RuntimeException("Unable to delete the user requested.");
        }
        return true;
    }
}
