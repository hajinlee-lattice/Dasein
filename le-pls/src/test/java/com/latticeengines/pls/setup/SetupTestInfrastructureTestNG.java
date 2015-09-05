package com.latticeengines.pls.setup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.EntityAccessRightsData;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public class SetupTestInfrastructureTestNG extends PlsFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(SetupTestInfrastructureTestNG.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = "infrastructure", enabled = true)
    public void setup() throws Exception {

        List<Tenant> tenants = getTenants(5);

        for (Tenant tenant : tenants) {
            tenantEntityMgr.delete(tenant);
        }

        boolean registeredUser = false;
        String externalUserName = getTheTestingUserAtLevel(AccessLevel.EXTERNAL_USER).getUsername();
        for (Tenant tenant : tenants) {
            try {
                createTenantByRestCall(tenant.getId());
                createAdminUserByRestCall(tenant.getId(), "admin", "build@lattice-engines.com", "Real", "Admin",
                        generalPasswordHash);
                createAdminUserByRestCall(tenant.getId(), adminUsername, adminUsername, "Super", "User",
                        adminPasswordHash);

                UserDocument userDoc = loginAndAttach(adminUsername, adminPassword, tenant);
                addAuthHeader.setAuthValue(userDoc.getTicket().getData());
                restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));

                if (!registeredUser) {
                    // Create user and add general rights.
                    UserRegistration commonUserReg = getUserRegistration(externalUserName, externalUserName, "General",
                            "User", generalPasswordHash);
                    String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", commonUserReg,
                            String.class);
                    ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json,
                            RegistrationResult.class);
                    Assert.assertNotNull(response);
                    String pwd = response.getResult().getPassword();
                    if (pwd != null) {
                        System.out.println("New password = " + pwd);
                    }

                    registeredUser = true;
                } else {
                    // Modify user and add general rights for the rest of the
                    // tenants
                    UserUpdateData data = new UserUpdateData();
                    Map<String, EntityAccessRightsData> rightsDataMap = new HashMap<>();
                    EntityAccessRightsData rightsData = new EntityAccessRightsData();
                    rightsData.setMayView(true);
                    rightsDataMap.put("PLS_Models", rightsData);
                    rightsDataMap.put("PLS_Configuration", rightsData);
                    rightsDataMap.put("PLS_Reporting", rightsData);
                    data.setRights(rightsDataMap);

                    String url = getRestAPIHostPort() + "/pls/users/" + externalUserName;
                    restTemplate.put(url, data, new HashMap<String, Object>());
                }
            } catch (Exception e) {
                log.info("Ignoring tenant registration error");
            }
        }

    }

    private UserRegistration getUserRegistration(String username, String email, String firstName, String lastName,
            String password) {
        UserRegistration userRegistration = new UserRegistration();
        User user = new User();
        user.setActive(true);
        user.setEmail(email);
        user.setFirstName(firstName);
        user.setLastName(lastName);
        user.setUsername(username);
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(password);
        userRegistration.setUser(user);
        userRegistration.setCredentials(creds);
        return userRegistration;
    }

    private List<Tenant> getTenants(int numTenants) {
        List<Tenant> tenants = new ArrayList<>();

        for (int i = 0; i < numTenants; i++) {
            Tenant tenant = new Tenant();
            String t = "TENANT" + (i + 1);
            tenant.setId(t);
            tenant.setName(t);
            tenants.add(tenant);
        }
        return tenants;
    }

    @Test(groups = "infrastructure")
    public void dummy() {
    }
}
