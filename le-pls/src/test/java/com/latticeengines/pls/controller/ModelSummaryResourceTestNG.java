package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
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
            grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, "NEW_TENANT", "admin");
            grantRight(GrantedRight.EDIT_PLS_CONFIGURATION, "NEW_TENANT", "admin");
            grantRight(GrantedRight.VIEW_PLS_REPORTING, "NEW_TENANT", "admin");
            grantRight(GrantedRight.VIEW_PLS_MODELS, "NEW_TENANT", "admin");
            grantRight(GrantedRight.EDIT_PLS_MODELS, "NEW_TENANT", "admin");
            grantRight(GrantedRight.EDIT_PLS_USERS, "NEW_TENANT", "admin");

            globalAuthenticationService.discard(ticket);
            ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        }

        assertEquals(ticket.getTenants().size(), 2);
        assertNotNull(ticket);
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();

        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant1, "admin");
        grantRight(GrantedRight.EDIT_PLS_CONFIGURATION, tenant1, "admin");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "admin");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant1, "admin");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant1, "admin");
        grantRight(GrantedRight.VIEW_PLS_USERS, tenant1, "admin");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant1, "admin");

        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant2, "admin");
        grantRight(GrantedRight.EDIT_PLS_CONFIGURATION, tenant2, "admin");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "admin");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "admin");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant2, "admin");
        grantRight(GrantedRight.VIEW_PLS_USERS, tenant2, "admin");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant2, "admin");

        if (globalUserManagementService.getUserByEmail("bnguyen@lattice-engines.com") == null) {
            assertTrue(globalUserManagementService.deleteUser("bnguyen"));
            assertTrue(globalUserManagementService.deleteUser("bnguyen@lattice-engines.com"));
            createUser("bnguyen", "bnguyen@lattice-engines.com", "Everything", "IsAwesome", "mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=");
        }
        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant1, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_CONFIGURATION, tenant1, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant1, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant1, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_USERS, tenant1, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant1, "bnguyen");

        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant2, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_CONFIGURATION, tenant2, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant2, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_USERS, tenant2, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant2, "bnguyen");

        if (globalUserManagementService.getUserByEmail("lming@lattice-engines.com") == null) {
            assertTrue(globalUserManagementService.deleteUser("lming"));
            assertTrue(globalUserManagementService.deleteUser("lming@lattice-engines.com"));
            createUser("lming", "lming@lattice-engines.com", "General", "User", "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=");
        }
        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant1, "lming");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "lming");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant1, "lming");

        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant2, "lming");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "lming");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "lming");


        if (globalUserManagementService.getUserByEmail("tsanghavi@lattice-engines.com") == null) {
            assertTrue(globalUserManagementService.deleteUser("tsanghavi@lattice-engines.com"));
            createUser("tsanghavi@lattice-engines.com", "tsanghavi@lattice-engines.com", "Tejas", "Sanghavi");
        }
        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant1, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.EDIT_PLS_CONFIGURATION, tenant1, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant1, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant1, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.VIEW_PLS_USERS, tenant1, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant1, "tsanghavi@lattice-engines.com");

        grantRight(GrantedRight.VIEW_PLS_CONFIGURATION, tenant2, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.EDIT_PLS_CONFIGURATION, tenant2, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant2, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.VIEW_PLS_USERS, tenant2, "tsanghavi@lattice-engines.com");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant2, "tsanghavi@lattice-engines.com");

        if (globalUserManagementService.getUserByEmail("rgonzalez@lattice-engines.com") == null) {
            assertTrue(globalUserManagementService.deleteUser("rgonzalez"));
            assertTrue(globalUserManagementService.deleteUser("rgonzalez@lattice-engines.com"));
            createUser("rgonzalez", "rgonzalez@lattice-engines.com", "Ron", "Gonzalez");
        }
        revokeRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "rgonzalez");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "rgonzalez");

        if (globalUserManagementService.getUserByEmail("ysong@lattice-engines.com") == null) {
            assertTrue(globalUserManagementService.deleteUser("ysong"));
            assertTrue(globalUserManagementService.deleteUser("ysong@lattice-engines.com"));
            createUser("ysong", "ysong@lattice-engines.com", "Yintao", "Song");
        }
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "ysong");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant1, "ysong");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant1, "ysong");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "ysong");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "ysong");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant2, "ysong");
        grantRight(GrantedRight.VIEW_PLS_USERS, tenant2, "ysong");

        setupDb(tenant1, tenant2);
    }

    @Test(groups = { "functional", "deployment" })
    public void getModelSummariesNoViewPlsModelsRight() {
        UserDocument doc = loginAndAttach("rgonzalez");
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

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
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

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
        UserDocument doc = loginAndAttach("bnguyen", "tahoe");
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
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
        UserDocument doc = loginAndAttach("bnguyen", "tahoe");
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
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
        UserDocument doc = loginAndAttach("bnguyen", "tahoe");
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
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
        UserDocument doc = loginAndAttach("bnguyen", "tahoe");
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        restTemplate.delete(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"));
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort()  + "/pls/modelsummaries/ms-8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a", ModelSummary.class);
        assertNull(summary);
    }

}
