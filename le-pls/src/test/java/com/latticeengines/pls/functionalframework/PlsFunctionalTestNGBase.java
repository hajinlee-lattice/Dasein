package com.latticeengines.pls.functionalframework;

import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTransactionalTestNGSpringContextTests;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalSessionManagementServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalUserManagementServiceImpl;
import com.latticeengines.pls.security.GrantedRight;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public class PlsFunctionalTestNGBase extends AbstractTransactionalTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(PlsFunctionalTestNGBase.class);
    
    @Autowired
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementServiceImpl globalSessionManagementService;
    
    @Autowired
    private GlobalUserManagementServiceImpl globalUserManagementService;
    
    protected RestTemplate restTemplate = new RestTemplate();
    
    protected void createUser(String username, String email, String firstName, String lastName) {
        try {
            User user1 = new User();
            user1.setFirstName(firstName);
            user1.setLastName(lastName);
            user1.setEmail(email);
            
            Credentials user1Creds = new Credentials();
            user1Creds.setUsername(username);
            user1Creds.setPassword("EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=");
            assertTrue(globalUserManagementService.registerUser(user1, user1Creds));
        } catch (Exception e) {
            log.info("User " + username + " already created.");
        }
    }
    
    protected void grantRight(GrantedRight right, String tenant, String username) {
        try {
            globalUserManagementService.grantRight(right.getAuthority(), tenant, username);
        } catch (Exception e) {
            log.info("Right " + right + " cannot be granted.");
        }
    }

    protected void revokeRight(GrantedRight right, String tenant, String username) {
        try {
            globalUserManagementService.revokeRight(right.getAuthority(), tenant, username);
        } catch (Exception e) {
            log.info("Right " + right + " cannot be revoked.");
        }
    }

    protected static class GetHttpStatusErrorHandler implements ResponseErrorHandler {
        
        public GetHttpStatusErrorHandler() {
        }

        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            if (response.getStatusCode() == HttpStatus.OK) {
                return false;
            }
            return true;
        }

        @Override
        public void handleError(ClientHttpResponse response) throws IOException {
            throw new RuntimeException("" + response.getStatusCode());
        }
    }

    
}
