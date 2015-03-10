package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalSessionManagementServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalUserManagementServiceImpl;
import com.latticeengines.pls.security.GrantedRight;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * This test has two users with particular privileges:
 *
 * rgonzalez - View_PLS_Reporting for tenant1
 * bnguyen - View_PLS_Reporting, View_PLS_Models for tenant2
 *
 * It ensures that rgonzalez cannot access any model summaries since it does not
 * have the View_PLS_Models right.
 *
 * It also ensures that bnguyen can indeed access the model summaries since it does
 * have the View_PLS_Models right.
 *
 * It also ensures that updates can only be done by bnguyen since this user
 * has Edit_PLS_Models right.
 *
 * @author rgonzalez
 *
 */
public class ModelSummaryResourceTestNG extends PlsFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ModelSummaryResourceTestNG.class);

    private Ticket ticket = null;
    private UserDocument adminDoc;

    @Autowired
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementServiceImpl globalSessionManagementService;

    @Autowired
    private GlobalUserManagementServiceImpl globalUserManagementService;

    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));

        if (ticket.getTenants().size() == 1) {
            Tenant newTenant = new Tenant();
            newTenant.setId("NEW_TENANT");
            newTenant.setName("NEW_TENANT");

            assertTrue(globalTenantManagementService.registerTenant(newTenant));
            grantRight(GrantedRight.VIEW_PLS_MODELS, "NEW_TENANT", "admin");

            globalAuthenticationService.discard(ticket);
            ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        }

        assertEquals(ticket.getTenants().size(), 2);
        assertNotNull(ticket);
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();

        // UI admin user
        grantAdminRights(tenant1, "admin");
        grantAdminRights(tenant2, "admin");

        // UI general user
        assertTrue(globalUserManagementService.deleteUser("ysong"));
        assertTrue(globalUserManagementService.deleteUser("ysong@lattice-engines.com"));
        createUser("ysong", "ysong@lattice-engines.com", "General", "User", "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=");
        grantDefaultRights(tenant1, "ysong");
        grantDefaultRights(tenant2, "ysong");

        // testing admin user
        User user = globalUserManagementService.getUserByEmail("bnguyen@lattice-engines.com");
        if (user == null || !user.getUsername().equals(adminUsername)) {
            assertTrue(globalUserManagementService.deleteUser("bnguyen"));
            assertTrue(globalUserManagementService.deleteUser("bnguyen@lattice-engines.com"));
            createUser(adminUsername, "bnguyen@lattice-engines.com", "Everything", "IsAwesome", adminPasswordHash);
        }
        grantAdminRights(tenant1, adminUsername);
        grantAdminRights(tenant2, adminUsername);

        // testing general user
        user = globalUserManagementService.getUserByEmail("lming@lattice-engines.com");
        if (user == null || !user.getUsername().equals(generalUsername)) {
            assertTrue(globalUserManagementService.deleteUser("lming"));
            assertTrue(globalUserManagementService.deleteUser("lming@lattice-engines.com"));
            createUser(generalUsername, "lming@lattice-engines.com", "General", "User", generalPasswordHash);
        }
        grantDefaultRights(tenant1, generalUsername);
        grantDefaultRights(tenant2, generalUsername);

        // PM admin user
        if (globalUserManagementService.getUserByEmail("tsanghavi@lattice-engines.com") == null) {
            assertTrue(globalUserManagementService.deleteUser("tsanghavi@lattice-engines.com"));
            createUser("tsanghavi@lattice-engines.com", "tsanghavi@lattice-engines.com", "Tejas", "Sanghavi");
        }
        grantAdminRights(tenant1, "tsanghavi@lattice-engines.com");
        grantAdminRights(tenant2, "tsanghavi@lattice-engines.com");

        // empty rights user
        if (globalUserManagementService.getUserByEmail("rgonzalez@lattice-engines.com") == null) {
            assertTrue(globalUserManagementService.deleteUser("rgonzalez"));
            assertTrue(globalUserManagementService.deleteUser("rgonzalez@lattice-engines.com"));
            createUser("rgonzalez", "rgonzalez@lattice-engines.com", "Ron", "Gonzalez");
        }
        revokeRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "rgonzalez");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "rgonzalez");

        setupDb(tenant1, tenant2);

        adminDoc = loginAndAttach(adminUsername, adminPassword);
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        globalAuthenticationService.discard(adminDoc.getTicket());
    }

    @BeforeMethod(groups = { "functional", "deployment" })
    public void beforeMethod() {
        // using admin session by default
        addAuthHeader.setAuthValue(adminDoc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
    }

    @Test(groups = { "functional", "deployment" })
    public void getModelSummariesNoViewPlsModelsRight() {
        UserDocument doc = loginAndAttach("rgonzalez");
        addAuthHeader.setAuthValue(doc.getTicket().getData());

        try {
            restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @Test(groups = { "functional", "deployment" })
    public void deleteModelSummaryNoEditPlsModelsRight() {
        UserDocument doc = loginAndAttach("rgonzalez");
        addAuthHeader.setAuthValue(doc.getTicket().getData());

        try {
            restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/123");
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "functional", "deployment" })
    public void getModelSummariesHasViewPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertEquals(summary.getName(), "PLSModel-Eloqua-02/18/2015 07:25:38 GMT");
        assertNotNull(summary.getDetails());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "getModelSummariesHasViewPlsModelsRight" })
    public void updateModelSummaryHasEditPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Name", "xyz");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertEquals(summary.getName(), "xyz");
        assertNotNull(summary.getDetails());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "updateModelSummaryHasEditPlsModelsRight" })
    public void updateAsDeletedModelSummaryHasEditPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);

        Map<String, String> map = (Map) response.get(0);
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsInactive");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

        attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsDeleted");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertNull(summary);

        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        int length1 = response.size();
        response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/?selection=all", List.class);
        assertNotNull(response);
        int length2 = response.size();
        assertEquals(length1 + 1, length2);

        attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsInactive");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());
        
        attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsActive");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "updateAsDeletedModelSummaryHasEditPlsModelsRight" })
    public void deleteModelSummaryHasEditPlsModelsRight() {
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"));
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort()  + "/pls/modelsummaries/ms-8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a", ModelSummary.class);
        assertNull(summary);
    }

}
