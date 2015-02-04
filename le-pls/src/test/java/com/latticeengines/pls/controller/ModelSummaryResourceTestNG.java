package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
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
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
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
        
        setupDb(tenant1, tenant2);
    }
    
    private void setupDb(String tenant1Name, String tenant2Name) {
        List<Tenant> tenants = tenantEntityMgr.findAll();

        for (Tenant tenant : tenants) {
            tenantEntityMgr.delete(tenant);
        }

        Tenant tenant1 = new Tenant();
        tenant1.setId(tenant1Name);
        tenant1.setName(tenant1Name);
        tenantEntityMgr.create(tenant1);
        
        ModelSummary summary1 = new ModelSummary();
        summary1.setId("123");
        summary1.setName("Model1");
        summary1.setTenant(tenant1);
        modelSummaryEntityMgr.create(summary1);

        Tenant tenant2 = new Tenant();
        tenant2.setId(tenant2Name);
        tenant2.setName(tenant2Name);
        tenantEntityMgr.create(tenant2);
        
        ModelSummary summary2 = new ModelSummary();
        summary2.setId("456");
        summary2.setName("Model2");
        summary2.setTenant(tenant2);
        Predictor s2p1 = new Predictor();
        s2p1.setApprovedUsage("Model");
        s2p1.setCategory("Construction");
        s2p1.setName("LeadSource");
        s2p1.setDisplayName("LeadSource");
        s2p1.setFundamentalType("");
        s2p1.setUncertaintyCoefficient(0.151911);
        summary2.addPredictor(s2p1);
        
        PredictorElement s2el1 = new PredictorElement();
        s2el1.setName("863d38df-d0f6-42af-ac0d-06e2b8a681f8");
        s2el1.setCorrelationSign(-1);
        s2el1.setCount(311L);
        s2el1.setLift(0.0);
        s2el1.setLowerInclusive(0.0);
        s2el1.setUpperExclusive(10.0);
        s2el1.setUncertaintyCoefficient(0.00313);
        s2el1.setRevenue(284788700000.0);
        s2el1.setVisible(true);
        s2p1.addPredictorElement(s2el1);

        PredictorElement s2el2 = new PredictorElement();
        s2el2.setName("7ade3995-f3da-4b83-87e6-c358ba3bdc00");
        s2el2.setCorrelationSign(1);
        s2el2.setCount(704L);
        s2el2.setLift(1.3884292375950742);
        s2el2.setLowerInclusive(10.0);
        s2el2.setUpperExclusive(1000.0);
        s2el2.setUncertaintyCoefficient(0.000499);
        s2el2.setRevenue(1682345087923.0);
        s2el2.setVisible(true);
        s2p1.addPredictorElement(s2el2);
        
        modelSummaryEntityMgr.create(summary2);
    }

    @Test(groups = "functional")
    public void getModelSummariesNoViewPlsModelsRight() {
        Credentials creds = new Credentials();
        creds.setUsername("rgonzalez");
        creds.setPassword(DigestUtils.sha256Hex("admin"));

        Session session = restTemplate.postForObject("http://localhost:8080/pls/user/login", creds, Session.class,
                new Object[] {});

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", session.getTicket().getData());

        HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);

        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        try {
            restTemplate.exchange( //
                    "http://localhost:8080/pls/modelsummaries/", HttpMethod.GET, entity, List.class, new HashMap<>());
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "403");
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "functional")
    public void getModelSummariesHasViewPlsModelsRight() {
        Credentials creds = new Credentials();
        creds.setUsername("bnguyen");
        creds.setPassword(DigestUtils.sha256Hex("admin"));

        Session session = restTemplate.postForObject("http://localhost:8080/pls/user/login", creds, Session.class,
                new Object[] {});

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", session.getTicket().getData());

        HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);
        ResponseEntity<List> response = restTemplate.exchange( //
                    "http://localhost:8080/pls/modelsummaries/", HttpMethod.GET, entity, List.class, new HashMap<>());
        assertNotNull(response);
        assertEquals(response.getBody().size(), 1);
        Map<String, String> map = (Map) response.getBody().get(0);
        ResponseEntity<ModelSummary> msResponse = restTemplate.exchange( //
                "http://localhost:8080/pls/modelsummaries/" + map.get("Id"), HttpMethod.GET, entity, ModelSummary.class, new HashMap<>());
        
        ModelSummary summary = msResponse.getBody(); 
        assertEquals(summary.getName(), "Model2");
        assertEquals(summary.getPredictors().size(), 1);
        assertEquals(summary.getPredictors().get(0).getPredictorElements().size(), 2);
    }

}
