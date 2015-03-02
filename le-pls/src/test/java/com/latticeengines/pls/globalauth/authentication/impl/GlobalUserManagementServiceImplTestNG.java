package com.latticeengines.pls.globalauth.authentication.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.GrantedRight;

import java.util.AbstractMap;
import java.util.List;

public class GlobalUserManagementServiceImplTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        UserDocument userDoc = loginAndAttach("admin");
        String tenant = userDoc.getTicket().getTenants().get(0).getId();
        createUser("abc@xyz.com", "abc@xyz.com", "Abc", "Def");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant, "abc@xyz.com");
    }

    @Test(groups = "functional")
    public void deleteUser() {
        UserDocument userDoc = loginAndAttach("abc@xyz.com");
        assertNotNull(userDoc);
        globalUserManagementService.deleteUser("abc@xyz.com");

        boolean exception = false;
        try {
            loginAndAttach("abc@xyz.com");
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);
    }

    @Test(groups = "functional")
    public void getUserByEmail() {
        UserDocument userDoc = loginAndAttach("admin");
        String tenant = userDoc.getTicket().getTenants().get(0).getId();
        createUser("abc@xyz.com", "abc@xyz.com", "Abc", "Def");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant, "abc@xyz.com");

        User user = globalUserManagementService.getUserByEmail("abc@xyz.com");
        assertNotNull(user);

        assertEquals(user.getEmail(), "abc@xyz.com");
        assertEquals(user.getUsername(), "abc@xyz.com");
        assertEquals(user.getFirstName(), "Abc");
        assertEquals(user.getLastName(), "Def");
        assertTrue(globalUserManagementService.deleteUser("abc@xyz.com"));
    }

    @Test(groups = "functional")
    public void resetLatticeCredentials() {
        UserDocument userDoc = loginAndAttach("admin");
        String tenant = userDoc.getTicket().getTenants().get(0).getId();
        createUser("test@xyz.com", "test@xyz.com", "Abc", "Def");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant, "test@xyz.com");
        userDoc = loginAndAttach("test@xyz.com");
        assertNotNull(userDoc);

        String newPassword = globalUserManagementService.resetLatticeCredentials("test@xyz.com");
        assertNotNull(newPassword);

        Ticket ticket = globalAuthenticationService.authenticateUser("test@xyz.com", DigestUtils.sha256Hex(newPassword));
        assertNotNull(ticket);
        assertEquals(ticket.getTenants().size(), 1);

        boolean result = globalAuthenticationService.discard(ticket);
        assertTrue(result);

        globalUserManagementService.deleteUser("test@xyz.com");
    }

    @Test(groups = "functional")
    public void getAllUsersForTenant() {
        UserDocument userDoc = loginAndAttach("admin");
        String tenant = userDoc.getTicket().getTenants().get(0).getId();

        int originalNumber = globalUserManagementService.getAllUsersOfTenant(tenant).size();

        String prefix = "Tester";
        String firstName = "First";
        String lastName = "Last";

        for (int i = 0; i < 10; i++) {
            String username = prefix + String.valueOf(i + 1);
            createUser(username, username+"@xyz.com", firstName, lastName);
            grantRight(GrantedRight.VIEW_PLS_MODELS, tenant, username);
            grantRight(GrantedRight.VIEW_PLS_USERS, tenant, username);
        }
        try {
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService.getAllUsersOfTenant(tenant);

            // this assertion may fail if multiple developers are testing against the same database simultaneously.
            assertEquals(userRightsList.size() - originalNumber, 10);

            for (AbstractMap.SimpleEntry<User, List<String>> userRight : userRightsList) {
                User user = userRight.getKey();
                if (user.getUsername().contains("Tester")) {
                    assertEquals(user.getEmail(), user.getUsername() + "@xyz.com");
                    assertEquals(user.getFirstName(), firstName);
                    assertEquals(user.getLastName(), lastName);

                    List<String> rights = userRight.getValue();
                    assertEquals(rights.size(), 2);
                    assertTrue(rights.contains(GrantedRight.VIEW_PLS_MODELS.getAuthority()));
                    assertTrue(rights.contains(GrantedRight.VIEW_PLS_USERS.getAuthority()));
                }
            }

        } finally {
            for (int i = 0; i < 10; i++) {
                globalUserManagementService.deleteUser(prefix + String.valueOf(i + 1));
            }
        }
    }
}
