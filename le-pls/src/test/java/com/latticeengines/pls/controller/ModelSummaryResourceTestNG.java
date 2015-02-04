package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;

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

    @SuppressWarnings("rawtypes")
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
    }

}
