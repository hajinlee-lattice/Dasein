package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalSessionManagementServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalUserManagementServiceImpl;
import com.latticeengines.pls.security.GrantedRight;

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
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        assertEquals(ticket.getTenants().size(), 2);
        assertNotNull(ticket);
        createUser("rgonzalez", "rgonzalez@lattice-engines.com", "Ron", "Gonzalez");
        createUser("bnguyen", "bnguyen@lattice-engines.com", "Bernie", "Nguyen");
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        revokeRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "rgonzalez");
        revokeRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "bnguyen");
        revokeRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "rgonzalez");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant2, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant1, "admin");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant2, "admin");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant1, "admin");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant2, "admin");

        createUser("ysong", "ysong@lattice-engines.com", "Yintao", "Song");
        revokeRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "ysong");
        revokeRight(GrantedRight.VIEW_PLS_MODELS, tenant1, "ysong");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "ysong");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant1, "ysong");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant1, "ysong");

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
        UserDocument doc = loginAndAttach("bnguyen");
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertEquals(summary.getName(), "PLSModel-Eloqua-02/18/2015 11:25:38");
        assertNotNull(summary.getDetails());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "getModelSummariesHasViewPlsModelsRight" })
    public void updateModelSummaryHasEditPlsModelsRight() {
        UserDocument doc = loginAndAttach("bnguyen");
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
        UserDocument doc = loginAndAttach("bnguyen");
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
        
        attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsActive");
        restTemplate.put(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());

    }    
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "updateAsDeletedModelSummaryHasEditPlsModelsRight" })
    public void deleteModelSummaryHasEditPlsModelsRight() {
        UserDocument doc = loginAndAttach("bnguyen");
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
