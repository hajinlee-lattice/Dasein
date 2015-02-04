package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.List;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.security.TicketAuthenticationToken;

public class ModelSummaryEntityMgrImplTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    private ModelSummary summary1;
    private ModelSummary summary2;
    private Long modelSummaryPid1;
    private Long modelSummaryPid2;
    
    
    @BeforeClass(groups = "functional")
    public void setup() {
        List<Tenant> tenants = tenantEntityMgr.findAll();

        for (Tenant tenant : tenants) {
            tenantEntityMgr.delete(tenant);
        }
        
        
        AbstractMap.SimpleEntry<Long, ModelSummary> s1 = createTenant1();
        AbstractMap.SimpleEntry<Long, ModelSummary> s2 = createTenant2();
        
        modelSummaryPid1 = s1.getKey();
        summary1 = s1.getValue();
        
        modelSummaryPid2 = s2.getKey();
        summary2 = s2.getValue();
    }
        
    private AbstractMap.SimpleEntry<Long, ModelSummary> createTenant1() {
        Tenant tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        summary1 = new ModelSummary();
        summary1.setId("123");
        summary1.setName("Model1");
        summary1.setTenant(tenant1);
        Predictor s1p1 = new Predictor();
        s1p1.setApprovedUsage("Model");
        s1p1.setCategory("Banking");
        s1p1.setName("LeadSource");
        s1p1.setDisplayName("LeadSource");
        s1p1.setFundamentalType("");
        s1p1.setUncertaintyCoefficient(0.151911);
        summary1.addPredictor(s1p1);
        
        PredictorElement s1el1 = new PredictorElement();
        s1el1.setName("863d38df-d0f6-42af-ac0d-06e2b8a681f8");
        s1el1.setCorrelationSign(-1);
        s1el1.setCount(311L);
        s1el1.setLift(0.0);
        s1el1.setLowerInclusive(0.0);
        s1el1.setUpperExclusive(10.0);
        s1el1.setUncertaintyCoefficient(0.00313);
        s1el1.setRevenue(284788700000.0);
        s1el1.setVisible(true);
        s1p1.addPredictorElement(s1el1);

        PredictorElement s1el2 = new PredictorElement();
        s1el2.setName("7ade3995-f3da-4b83-87e6-c358ba3bdc00");
        s1el2.setCorrelationSign(1);
        s1el2.setCount(704L);
        s1el2.setLift(1.3884292375950742);
        s1el2.setLowerInclusive(10.0);
        s1el2.setUpperExclusive(1000.0);
        s1el2.setUncertaintyCoefficient(0.000499);
        s1el2.setRevenue(1682345087923.0);
        s1el2.setVisible(true);
        s1p1.addPredictorElement(s1el2);
        
        modelSummaryEntityMgr.create(summary1);
        return new AbstractMap.SimpleEntry<>(summary1.getPid(), summary1);
    }
    
    private AbstractMap.SimpleEntry<Long, ModelSummary> createTenant2() {
        Tenant tenant2 = new Tenant();
        tenant2.setId("TENANT2");
        tenant2.setName("TENANT2");
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
        return new AbstractMap.SimpleEntry<>(summary2.getPid(), summary2);
    }
    
    @Test(groups = "functional")
    public void findByKey() {
        ModelSummary key = new ModelSummary();
        key.setPid(modelSummaryPid1);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.findByKey(key);
        List<Predictor> predictors = retrievedSummary.getPredictors();
        
        assertEquals(retrievedSummary.getId(), summary1.getId());
        assertEquals(retrievedSummary.getName(), summary1.getName());
        assertEquals(predictors.size(), 1);
        
        String[] predictorFields = new String[] {
                "name", //
                "displayName", //
                "approvedUsage", //
                "category", //
                "fundamentalType", //
                "uncertaintyCoefficient"
        };
        
        String[] predictorElementFields = new String[] {
                "name", //
                "correlationSign", //
                "count", //
                "lift", //
                "lowerInclusive", //
                "upperExclusive", //
                "uncertaintyCoefficient", //
                "revenue", //
                "visible"
        };

        for (int i = 0; i < predictors.size(); i++) {
            for (String field : predictorFields) {
                assertEquals(ReflectionTestUtils.getField(predictors.get(i), field),
                        ReflectionTestUtils.getField(summary1.getPredictors().get(i), field));
                
            }
            List<PredictorElement> retrievedElements = predictors.get(i).getPredictorElements(); 
            List<PredictorElement> summaryElements = summary1.getPredictors().get(i).getPredictorElements();
            for (int j = 0; j < retrievedElements.size(); j++) {
                for (String field : predictorElementFields) {
                    assertEquals(ReflectionTestUtils.getField(retrievedElements.get(j), field),
                            ReflectionTestUtils.getField(summaryElements.get(j), field));
                    
                }
            }
        }
    }
    
    @Test(groups = "functional")
    public void findAll() {
        SecurityContext securityContext = Mockito.mock(SecurityContext.class);
        TicketAuthenticationToken token = Mockito.mock(TicketAuthenticationToken.class);
        Session session = Mockito.mock(Session.class);
        Tenant tenant = Mockito.mock(Tenant.class);
        Mockito.when(session.getTenant()).thenReturn(tenant);
        Mockito.when(tenant.getId()).thenReturn(summary2.getTenant().getId());
        Mockito.when(token.getSession()).thenReturn(session);
        Mockito.when(securityContext.getAuthentication()).thenReturn(token);
        SecurityContextHolder.setContext(securityContext);
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
        assertEquals(summaries.size(), 1);
        assertEquals(summaries.get(0).getName(), summary2.getName());
    }
    
}
