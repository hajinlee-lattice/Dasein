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
import com.latticeengines.domain.exposed.security.Session;
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
    
    @BeforeClass(groups = "functional")
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
        
        setupDb(tenant1, tenant2);
    }
    
    @Test(groups = "functional", enabled = true)
    public void getModelSummariesNoViewPlsModelsRight() {
        Session session = login("rgonzalez");
        addAuthHeader.setAuthValue(session.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        try {
            restTemplate.getForObject("http://localhost:8080/pls/modelsummaries/", List.class);
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @Test(groups = "functional", enabled = true)
    public void deleteModelSummaryNoEditPlsModelsRight() {
        Session session = login("rgonzalez");
        addAuthHeader.setAuthValue(session.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        try {
            restTemplate.delete("http://localhost:8080/pls/modelsummaries/123");
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "functional", enabled = true)
    public void getModelSummariesHasViewPlsModelsRight() {
        Session session = login("bnguyen");
        addAuthHeader.setAuthValue(session.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
        List response = restTemplate.getForObject("http://localhost:8080/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        ModelSummary summary = restTemplate.getForObject("http://localhost:8080/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertEquals(summary.getName(), "Model2");
        assertNotNull(summary.getDetails());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "functional", dependsOnMethods = { "getModelSummariesHasViewPlsModelsRight" })
    public void updateModelSummaryHasEditPlsModelsRight() {
        Session session = login("bnguyen");
        addAuthHeader.setAuthValue(session.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
        List response = restTemplate.getForObject("http://localhost:8080/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
        Map<String, String> map = (Map) response.get(0);
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Name", "xyz");
        restTemplate.put("http://localhost:8080/pls/modelsummaries/" + map.get("Id"), attrMap, new HashMap<>());
        
        ModelSummary summary = restTemplate.getForObject("http://localhost:8080/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertEquals(summary.getName(), "xyz");
        assertNotNull(summary.getDetails());
    }
    
    @Test(groups = "functional", dependsOnMethods = { "updateModelSummaryHasEditPlsModelsRight" })
    public void deleteModelSummaryHasEditPlsModelsRight() {
        Session session = login("bnguyen");
        addAuthHeader.setAuthValue(session.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        restTemplate.delete("http://localhost:8080/pls/modelsummaries/456");
        ModelSummary summary = restTemplate.getForObject("http://localhost:8080/pls/modelsummaries/456", ModelSummary.class);
        assertNull(summary);
    }
    
}
