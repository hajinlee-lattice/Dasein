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
            log.info("Registering user " + creds.getUsername() + " against Global Auth.");
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
            log.info("Granting right " + right + " to user " + username + " for tenant " + tenant + ".");
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
            log.info("Revoking right " + right + " to user " + username + " for tenant " + tenant + ".");
            return service.revokeRight(right, tenant, username);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18006, e, new String[] { right, username, tenant });
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

}
