package com.latticeengines.security.exposed.globalauth.impl;

import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.annotation.RestApiCall;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.EmailSettings;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.globalauth.generated.usermgr.IUserManagementService;
import com.latticeengines.security.globalauth.generated.usermgr.ObjectFactory;
import com.latticeengines.security.globalauth.generated.usermgr.SOAPEmailSettings;
import com.latticeengines.security.globalauth.generated.usermgr.UserManagementService;

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

    @RestApiCall
    private IUserManagementService getService() {
        SSLUtils.turnOffSslChecking();
        UserManagementService service;
        try {
            service = new UserManagementService(new URL(globalAuthUrl + "/GlobalAuthUserManager?wsdl"));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18000, e, new String[] { globalAuthUrl });
        }
        return service.getBasicHttpBindingIUserManagementService();
    }

    @Override
    @RestApiCall
    public synchronized Boolean registerUser(User user, Credentials creds) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Registering user %s against Global Auth.", creds.getUsername()));
            return service.registerUser(new SoapUserBuilder(user).build(), new SoapCredentialsBuilder(creds).build());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18004, e, new String[] { creds.getUsername() });
        }
    }

    @Override
    @RestApiCall
    public synchronized Boolean grantRight(String right, String tenant, String username) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Granting right %s to user %s for tenant %s.", right, username, tenant));
            return service.grantRight(right, tenant, username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18005, e, new String[] { right, username, tenant });
        }
    }

    @Override
    @RestApiCall
    public List<String> getRights(String username, String tenantId) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Getting rights of user %s in tenant %s.", username, tenantId));
            return service.getRights(tenantId, username).getString();
        } catch (Exception e) {
            if (e.getMessage().contains("Sequence contains no elements")) {
                return new ArrayList<>();
            }
            throw new LedpException(LedpCode.LEDP_18000,
                "Getting rights of user " + username + " in tenant " + tenantId + ".", e);
        }
    }

    @Override
    @RestApiCall
    public synchronized Boolean revokeRight(String right, String tenant, String username) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Revoking right %s from user %s for tenant %s.", right, username, tenant));
            return service.revokeRight(right, tenant, username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18006, e, new String[] { right, username, tenant });
        }
    }

    @Override
    @RestApiCall
    public synchronized Boolean forgotLatticeCredentials(String username) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);

        // Validate that username exists before reset
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
            return service.forgotLatticeCredentialsBySettings(username, new SoapEmailSettingsBuilder(emailsettings).build());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18011, e, new String[] { username });
        }
    }

    @Override
    @RestApiCall
    public synchronized Boolean modifyLatticeCredentials(Ticket ticket, Credentials oldCreds, Credentials newCreds) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Modifying credentials for %s.", oldCreds.getUsername()));
            return service.modifyLatticeCredentials(
                new SoapTicketBuilder(ticket).build(),
                new SoapCredentialsBuilder(oldCreds).build(),
                new SoapCredentialsBuilder(newCreds).build()
            );
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18010, e, new String[] { oldCreds.getUsername() });
        }
    }

    @Override
    @RestApiCall
    public synchronized String resetLatticeCredentials(String username) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Resetting credentials for %s.", username));
            return service.resetLatticeCredentials(username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18011, e, new String[] { username });
        }
    }

    @Override
    @RestApiCall
    public User getUserByEmail(String email) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Getting user having the email %s.", email));
            return new UserBuilder(service.findUserByEmail(email)).build();
        } catch (Exception e) {
            //throw new LedpException(LedpCode.LEDP_18017, e, new String[] { email });
            //TODO: handle different exceptions returned from GlobalAuth
            return null;
        }
    }

    @Override
    @RestApiCall
    public User getUserByUsername(String username) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Getting user %s.", username));
            return new UserBuilder(service.findUserByUsername(username)).build();
        } catch (Exception e) {
            //throw new LedpException(LedpCode.LEDP_18017, e, new String[] { email });
            //TODO: handle different exceptions returned from GlobalAuth
            return null;
        }
    }

    @Override
    @RestApiCall
    public synchronized Boolean deleteUser(String username) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            boolean result = service.deleteUser(username);
            log.info(String.format("Deleting user %s success = %s", username, result));
            return result;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18015, e, new String[] { username });
        }
    }

    @Override
    @RestApiCall
    public Boolean isRedundant(String username) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Checking if user %s is redundant.", username));
            return service.isRedundant(username);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    @RestApiCall
    public List<AbstractMap.SimpleEntry<User, List<String>>> getAllUsersOfTenant(String tenantId) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Getting all users and their rights for tenant %s.", tenantId));
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = new ArrayList<>();
            for (com.latticeengines.security.globalauth.generated.usermgr.UserRights userRights : service
                    .findAllUserRightsByTenant(tenantId).getUserRights()) {
                userRightsList.add(new UserRightsBuilder(userRights).build());
            }
            return userRightsList;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18016, e, new String[] { tenantId });
        }
    }

    static class SoapUserBuilder {
        private User user;

        public SoapUserBuilder(User user) {
            this.user = user;
        }

        public com.latticeengines.security.globalauth.generated.usermgr.User build() {
            com.latticeengines.security.globalauth.generated.usermgr.User u = new ObjectFactory().createUser();
            u.setEmail(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "Email"), //
                    String.class, user.getEmail()));
            u.setFirstName(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "FirstName"), //
                    String.class, user.getFirstName()));
            u.setLastName(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "LastName"), //
                    String.class, user.getLastName()));
            u.setPhoneNumber(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "PhoneNumber"), //
                    String.class, user.getPhoneNumber()));
            u.setTitle(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "Title"), //
                    String.class, user.getTitle()));
            return u;
        }
    }

    static class SoapCredentialsBuilder {
        private Credentials creds;

        public SoapCredentialsBuilder(Credentials creds) {
            this.creds = creds;
        }

        public com.latticeengines.security.globalauth.generated.usermgr.Credentials build() {
            com.latticeengines.security.globalauth.generated.usermgr.Credentials c = new ObjectFactory().createCredentials();
            c.setUsername(new JAXBElement<String>( //
                new QName("http://schemas.lattice-engines.com/2008/Poet", "Username"), //
                String.class, creds.getUsername()));
            c.setPassword(new JAXBElement<String>( //
                new QName("http://schemas.lattice-engines.com/2008/Poet", "Password"), //
                String.class, creds.getPassword()));

            return c;

        }
    }

    static class SoapTicketBuilder {
        private Ticket ticket;

        public SoapTicketBuilder(Ticket ticket) {
            this.ticket = ticket;
        }

        public com.latticeengines.security.globalauth.generated.usermgr.Ticket build() {
            com.latticeengines.security.globalauth.generated.usermgr.Ticket t = new ObjectFactory().createTicket();
            t.setUniquness(ticket.getUniqueness());
            t.setRandomness(new JAXBElement<String>( //
                new QName("http://schemas.lattice-engines.com/2008/Poet", "Randomness"), //
                String.class, ticket.getRandomness()));
            return t;
        }
    }

    static class SoapEmailSettingsBuilder {
        private EmailSettings emailSettings;

        public SoapEmailSettingsBuilder(EmailSettings emailSettings) {
            this.emailSettings = emailSettings;
        }

        public SOAPEmailSettings build() {
            SOAPEmailSettings s = new ObjectFactory().createSOAPEmailSettings();
            s.setFrom(new JAXBElement<String>( //
                new QName("http://schemas.lattice-engines.com/2008/Poet", "From"), //
                String.class, emailSettings.getFrom()));
            s.setPassword(new JAXBElement<String>( //
                new QName("http://schemas.lattice-engines.com/2008/Poet", "Password"), //
                String.class, emailSettings.getPassword()));
            s.setServer(new JAXBElement<String>( //
                new QName("http://schemas.lattice-engines.com/2008/Poet", "Server"), //
                String.class, emailSettings.getServer()));
            s.setUsername(new JAXBElement<String>( //
                new QName("http://schemas.lattice-engines.com/2008/Poet", "Username"), //
                String.class, emailSettings.getUsername()));
            s.setPort(emailSettings.getPort());
            s.setUseSSL(emailSettings.isUseSSL());
            return s;
        }
    }

    static class UserBuilder {
        private com.latticeengines.security.globalauth.generated.usermgr.User user;

        public UserBuilder(com.latticeengines.security.globalauth.generated.usermgr.User user) {
            this.user = user;
        }

        public User build() {
            User u = new User();

            u.setActive(user.getIsActive().getValue());
            u.setEmail(user.getEmail().getValue());
            u.setFirstName(user.getFirstName().getValue());
            u.setLastName(user.getLastName().getValue());
            u.setPhoneNumber(user.getPhoneNumber().getValue());
            u.setTitle(user.getTitle().getValue());
            u.setUsername(user.getUsername().getValue());

            return u;
        }
    }

    static class UserRightsBuilder {
        private com.latticeengines.security.globalauth.generated.usermgr.UserRights userRights;

        public UserRightsBuilder(com.latticeengines.security.globalauth.generated.usermgr.UserRights userRights) {
            this.userRights = userRights;
        }

        public AbstractMap.SimpleEntry<User, List<String>> build() {
            return new AbstractMap.SimpleEntry<>(new UserBuilder(userRights.getUser()).build(), userRights.getRights()
                    .getValue().getString());
        }
    }

}
