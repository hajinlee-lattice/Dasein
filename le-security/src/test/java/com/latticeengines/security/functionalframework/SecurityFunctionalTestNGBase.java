package com.latticeengines.security.functionalframework;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-security-context.xml" })
public class SecurityFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(SecurityFunctionalTestNGBase.class);

    protected static boolean usersInitialized = false;
    protected static final String adminUsername = "bnguyen@lattice-engines.com";
    protected static final String adminPassword = "tahoe";
    protected static final String adminPasswordHash = "mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=";
    protected static final String generalUsername = "lming@lattice-engines.com";
    protected static final String generalPassword = "admin";
    protected static final String generalPasswordHash = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    protected void createUser(String username, String email, String firstName, String lastName) {
        createUser(username, email, firstName, lastName, generalPasswordHash);
    }

    protected void createUser(String username, String email, String firstName, String lastName, String password) {
        try {
            User user1 = new User();
            user1.setFirstName(firstName);
            user1.setLastName(lastName);
            user1.setEmail(email);

            Credentials user1Creds = new Credentials();
            user1Creds.setUsername(username);
            user1Creds.setPassword(password);
            assertTrue(globalUserManagementService.registerUser(user1, user1Creds));
        } catch (Exception e) {
            log.info("User " + username + " already created.");
        }
    }

    protected void makeSureUserDoesNotExist(String username) {
        assertTrue(globalUserManagementService.deleteUser(username));
        assertNull(globalUserManagementService.getUserByUsername(username));
    }
    
    protected Session loginAndAttach(String username) {
        return loginAndAttach(username, generalPassword);
    }
    
    protected Session loginAndAttach(String username, String password) {
        password = DigestUtils.sha256Hex(password);
        System.out.println(password);
        Ticket ticket = globalAuthenticationService.authenticateUser(username, password);
        return globalSessionManagementService.attach(ticket);
    }


}
