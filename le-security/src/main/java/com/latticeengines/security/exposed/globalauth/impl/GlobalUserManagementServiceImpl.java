package com.latticeengines.security.exposed.globalauth.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.EmailSettings;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.entitymanager.GlobalAuthAuthenticationEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.security.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.util.GlobalAuthPasswordUtils;

@Component("globalUserManagementService")
public class GlobalUserManagementServiceImpl extends GlobalAuthenticationServiceBaseImpl implements
        GlobalUserManagementService {

    private static final Log log = LogFactory.getLog(GlobalUserManagementServiceImpl.class);

    @Value("${security.emailsettings.from}")
    private String EMAIL_FROM;

    @Value("${security.emailsettings.server}")
    private String EMAIL_SERVER;

    @Value("${security.emailsettings.username}")
    private String EMAIL_USERNAME;

    @Value("${security.emailsettings.password}")
    private String EMAIL_PASSWORD;

    @Value("${security.emailsettings.port}")
    private int EMAIL_PORT;

    @Value("${security.emailsettings.useSSL}")
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
            if (e.getMessage().contains("Sequence contains no elements")) {
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

        String password = GlobalAuthPasswordUtils.GetSecureRandomString(16);
        latticeAuthenticationData.setPassword(GlobalAuthPasswordUtils
                .EncryptPassword(GlobalAuthPasswordUtils.Hash256(password)));
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
                .EncryptPassword(oldCreds.getPassword()))) {
            throw new Exception("Old password is incorrect for password change.");
        }
        if (oldCreds.getPassword().equals(newCreds.getPassword())) {
            throw new Exception("The new password cannot be the same as the old password.");
        }
        latticeAuthenticationData.setPassword(GlobalAuthPasswordUtils.EncryptPassword(newCreds
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
        password = GlobalAuthPasswordUtils.GetSecureRandomString(16);
        latticeAuthenticationData.setPassword(GlobalAuthPasswordUtils
                .EncryptPassword(GlobalAuthPasswordUtils.Hash256(password)));
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
            log.info(String.format("Checking if user %s is redundant.", username));
            return globalAuthIsRedundant(username);
        } catch (Exception e) {
            return false;
        }
    }

    private Boolean globalAuthIsRedundant(String username) throws Exception {
        User user = globalAuthFindUserByUsername(username);
        List<GlobalAuthUser> userDatas = gaUserEntityMgr
                .findByEmailJoinUserTenantRight(user.getEmail());
        Set<String> distinctRights = new HashSet<String>();
        for (GlobalAuthUser userData : userDatas) {
            if (userData.getUserTenantRights() != null && userData.getUserTenantRights().size() > 0) {
                for (GlobalAuthUserTenantRight rightData : userData.getUserTenantRights()) {
                    distinctRights.add(rightData.getOperationName());
                }
            }
        }
        return distinctRights.size() == 0;
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
        List<GlobalAuthUser> userDatas = gaUserEntityMgr
                .findByTenantIdJoinAuthenticationJoinUserTenantRight(tenantData.getPid());
        if (userDatas == null || userDatas.size() == 0) {
            throw new Exception("Unable to find any user associated with the tenant requested.");
        }
        for (GlobalAuthUser userData : userDatas) {
            User user = new User();
            GlobalAuthAuthentication authData = gaUserEntityMgr
                    .findByUserIdWithTenantRightsAndAuthentications(userData.getPid())
                    .getAuthentications().get(0);
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

            AbstractMap.SimpleEntry<User, List<String>> uRights;
            List<GlobalAuthUserTenantRight> rightsData = gaUserTenantRightEntityMgr
                    .findByUserIdAndTenantId(userData.getPid(), tenantData.getPid());
            if (rightsData != null) {

                Set<String> distinctRights = new HashSet<String>();
                for (GlobalAuthUserTenantRight rightData : rightsData) {
                    distinctRights.add(rightData.getOperationName());
                }
                List<String> rights = new ArrayList<String>(distinctRights);
                uRights = new AbstractMap.SimpleEntry<>(user, rights);
            } else {
                uRights = new AbstractMap.SimpleEntry<>(user, null);
            }

            userRightsList.add(uRights);

        }
        return userRightsList;
    }
}
