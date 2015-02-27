package com.latticeengines.pls.globalauth.authentication.impl;

import java.net.URL;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.globalauth.generated.usermgr.IUserManagementService;
import com.latticeengines.pls.globalauth.generated.usermgr.ObjectFactory;
import com.latticeengines.pls.globalauth.generated.usermgr.UserManagementService;

@Component("globalUserManagementService")
public class GlobalUserManagementServiceImpl extends GlobalAuthenticationServiceBaseImpl implements GlobalUserManagementService {
    
    private static final Log log = LogFactory.getLog(GlobalUserManagementServiceImpl.class);

    private IUserManagementService getService() {
        UserManagementService service;
        try {
            service = new UserManagementService(new URL(globalAuthUrl + "/GlobalAuthUserManager?wsdl"));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18000, e, new String[] { globalAuthUrl });
        }
        return service.getBasicHttpBindingIUserManagementService();
    }


    
    @Override
    public Boolean registerUser(User user, Credentials creds) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Registering user %s against Global Auth.", creds.getUsername()));
            return service.registerUser(
                    new SoapUserBuilder(user).build(),
                    new SoapCredentialsBuilder(creds).build());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18004, e, new String[] { creds.getUsername() });
        }
    }

    @Override
    public Boolean grantRight(String right, String tenant, String username) {
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
    public Boolean revokeRight(String right, String tenant, String username) {
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
    public Boolean forgotLatticeCredentials(String username, String tenantId) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Resetting credentials for user %s and tenant %s.", username, tenantId));
            String deploymentId = tenantId;
            return service.forgotLatticeCredentials(username, deploymentId);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18011, e, new String[] { username, tenantId });
        }
    }

    @Override
    public Boolean modifyLatticeCredentials(Ticket ticket, Credentials oldCreds, Credentials newCreds) {
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
            throw new LedpException(LedpCode.LEDP_18010, e, new String[] {oldCreds.getUsername()});
        }
    }
    
    @Override
    public Boolean deleteUser(String username) {
        IUserManagementService service = getService();
        addMagicHeaderAndSystemProperty(service);
        try {
            log.info(String.format("Deleting user %s.", username));
            return service.deleteUser(username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18015, e, new String[] { username });
        }
    }

    static class SoapUserBuilder {
        private User user;
        
        public SoapUserBuilder(User user) {
            this.user = user;
        }
        
        public com.latticeengines.pls.globalauth.generated.usermgr.User build() {
            com.latticeengines.pls.globalauth.generated.usermgr.User u = new ObjectFactory().createUser();
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

        public com.latticeengines.pls.globalauth.generated.usermgr.Credentials build() {
            com.latticeengines.pls.globalauth.generated.usermgr.Credentials c = new ObjectFactory().createCredentials();
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

        public com.latticeengines.pls.globalauth.generated.usermgr.Ticket build() {
            com.latticeengines.pls.globalauth.generated.usermgr.Ticket t = new ObjectFactory().createTicket();
            t.setUniquness(ticket.getUniqueness());
            t.setRandomness(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "Randomness"), //
                    String.class, ticket.getRandomness()));
            return t;
        }
    }

    static class SoapTenantBuilder {
        private Tenant tenant;

        public SoapTenantBuilder(Tenant tenant) {
            this.tenant = tenant;
        }

        public com.latticeengines.pls.globalauth.generated.usermgr.Tenant build() {
            com.latticeengines.pls.globalauth.generated.usermgr.Tenant t = new ObjectFactory().createTenant();
            t.setIdentifier(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "Identifier"), //
                    String.class, tenant.getId()));
            t.setDisplayName(new JAXBElement<String>( //
                    new QName("http://schemas.lattice-engines.com/2008/Poet", "DisplayName"), //
                    String.class, tenant.getName()));
            return t;
        }
    }

}
