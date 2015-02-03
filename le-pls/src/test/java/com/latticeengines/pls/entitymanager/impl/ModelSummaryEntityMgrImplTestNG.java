package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ModelSummaryEntityMgrImplTestNG extends PlsFunctionalTestNGBase {
    
    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    private ModelSummary summary;
    private Long modelSummaryPid;
    
    
    @BeforeClass(groups = "functional")
    public void setup() {
        List<Tenant> tenants = tenantEntityMgr.findAll();
        
        for (Tenant tenant : tenants) {
            tenantEntityMgr.delete(tenant);
        }
        
        Tenant tenant = new Tenant();
        tenant.setId("TENANT1");
        tenant.setName("TENANT1");
        summary = new ModelSummary();
        summary.setId("123");
        summary.setName("This is a model");
        summary.setTenant(tenant);
        Predictor p1 = new Predictor();
        p1.setApprovedUsage("Model");
        p1.setCategory("XYZ");
        p1.setName("LeadSource");
        p1.setDisplayName("LeadSource");
        p1.setFundamentalType("");
        p1.setUncertaintyCoefficient(0.151911);
        summary.addPredictor(p1);
        
        PredictorElement el1 = new PredictorElement();
        el1.setName("863d38df-d0f6-42af-ac0d-06e2b8a681f8");
        el1.setCorrelationSign(-1);
        el1.setCount(311L);
        el1.setLift(0.0);
        el1.setLowerInclusive(0.0);
        el1.setUpperExclusive(10.0);
        el1.setUncertaintyCoefficient(0.00313);
        el1.setRevenue(284788700000.0);
        el1.setVisible(true);
        p1.addPredictorElement(el1);

        PredictorElement el2 = new PredictorElement();
        el2.setName("7ade3995-f3da-4b83-87e6-c358ba3bdc00");
        el2.setCorrelationSign(1);
        el2.setCount(704L);
        el2.setLift(1.3884292375950742);
        el2.setLowerInclusive(10.0);
        el2.setUpperExclusive(1000.0);
        el2.setUncertaintyCoefficient(0.000499);
        el2.setRevenue(1682345087923.0);
        el2.setVisible(true);
        p1.addPredictorElement(el2);
        
        modelSummaryEntityMgr.create(summary);
        modelSummaryPid = summary.getPid();
    }
    
    @Test(groups = "functional")
    private void findByKey() {
        ModelSummary key = new ModelSummary();
        key.setPid(modelSummaryPid);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.findByKey(key);
        List<Predictor> predictors = retrievedSummary.getPredictors();
        
        assertEquals(retrievedSummary.getId(), summary.getId());
        assertEquals(retrievedSummary.getName(), summary.getName());
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
                        ReflectionTestUtils.getField(summary.getPredictors().get(i), field));
                
            }
            List<PredictorElement> retrievedElements = predictors.get(i).getPredictorElements(); 
            List<PredictorElement> summaryElements = summary.getPredictors().get(i).getPredictorElements();
            for (int j = 0; j < retrievedElements.size(); j++) {
                for (String field : predictorElementFields) {
                    assertEquals(ReflectionTestUtils.getField(retrievedElements.get(j), field),
                            ReflectionTestUtils.getField(summaryElements.get(j), field));
                    
                }
            }
        }
    }
    
}
