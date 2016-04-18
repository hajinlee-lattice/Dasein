package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.entitymanager.KeyValueEntityMgr;

public class ModelSummaryEntityMgrImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private KeyValueEntityMgr keyValueEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;

    private ModelSummary summary1;
    private ModelSummary summary2;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        Tenant tenant2 = tenantService.findByTenantId("TENANT2");

        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }

        summary1 = createModelSummaryForTenant1();
        summary2 = createModelSummaryForTenant2();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        Tenant tenant2 = tenantService.findByTenantId("TENANT2");

        tenantService.discardTenant(tenant1);
        tenantService.discardTenant(tenant2);
    }

    private void setDetails(ModelSummary summary) throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/pls/functionalframework/modelsummary-marketo.json");
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        details.setData(data);
        summary.setDetails(details);
    }

    private ModelSummary createModelSummaryForTenant1() throws Exception {
        Tenant tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        summary1 = new ModelSummary();
        summary1.setId("123");
        summary1.setName("Model1");
        summary1.setRocScore(0.75);
        summary1.setLookupId("TENANT1|Q_EventTable_TENANT1|abcde");
        summary1.setTrainingRowCount(8000L);
        summary1.setTestRowCount(2000L);
        summary1.setTotalRowCount(10000L);
        summary1.setTrainingConversionCount(80L);
        summary1.setTestConversionCount(20L);
        summary1.setTotalConversionCount(100L);
        summary1.setConstructionTime(System.currentTimeMillis());
        summary1.setTenant(tenant1);
        setDetails(summary1);
        Predictor s1p1 = new Predictor();
        s1p1.setApprovedUsage("Model");
        s1p1.setCategory("Banking");
        s1p1.setName("LeadSource");
        s1p1.setDisplayName("LeadSource");
        s1p1.setFundamentalType("");
        s1p1.setUncertaintyCoefficient(0.151911);
        s1p1.setUsedForBuyerInsights(true);
        summary1.addPredictor(s1p1);
        Predictor s1p2 = new Predictor();
        s1p2.setApprovedUsage("ModelAndModelInsights");
        s1p2.setCategory("Banking");
        s1p2.setName("Website_Custom");
        s1p2.setDisplayName("Website_Custom");
        s1p2.setFundamentalType("");
        s1p2.setUncertaintyCoefficient(0.251911);
        s1p2.setUsedForBuyerInsights(true);
        summary1.addPredictor(s1p2);
        Predictor s1p3 = new Predictor();
        s1p3.setApprovedUsage("ModelAndModelInsights");
        s1p3.setCategory("Finance");
        s1p3.setName("Income");
        s1p3.setDisplayName("Income");
        s1p3.setFundamentalType("numeric");
        s1p3.setUncertaintyCoefficient(0.171911);
        s1p3.setUsedForBuyerInsights(false);
        summary1.addPredictor(s1p3);

        PredictorElement s1el1 = new PredictorElement();
        s1el1.setName("863d38df-d0f6-42af-ac0d-06e2b8a681f8");
        s1el1.setCorrelationSign(-1);
        s1el1.setCount(311L);
        s1el1.setLift(0.0);
        s1el1.setLowerInclusive(0.0);
        s1el1.setUpperExclusive(10.0);
        s1el1.setUncertaintyCoefficient(0.00313);
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
        s1el2.setVisible(true);
        s1p1.addPredictorElement(s1el2);

        modelSummaryEntityMgr.create(summary1);
        return summary1;
    }

    private ModelSummary createModelSummaryForTenant2() throws Exception {
        Tenant tenant2 = new Tenant();
        tenant2.setId("TENANT2");
        tenant2.setName("TENANT2");
        tenantEntityMgr.create(tenant2);
        ModelSummary summary2 = new ModelSummary();
        summary2.setId("456");
        summary2.setName("Model2");
        summary2.setRocScore(0.80);
        summary2.setLookupId("TENANT2|Q_EventTable_TENANT2|fghij");
        summary2.setTrainingRowCount(80000L);
        summary2.setTestRowCount(20000L);
        summary2.setTotalRowCount(100000L);
        summary2.setTrainingConversionCount(800L);
        summary2.setTestConversionCount(200L);
        summary2.setTotalConversionCount(1000L);
        summary2.setConstructionTime(System.currentTimeMillis());
        summary2.setTenant(tenant2);
        setDetails(summary2);
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
        s2el2.setVisible(true);
        s2p1.addPredictorElement(s2el2);

        modelSummaryEntityMgr.create(summary2);
        return summary2;
    }

    @Test(groups = "functional")
    public void findByModelId() {
        setupSecurityContext(summary1);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.findByModelId(summary1.getId(), true, true, false);
        assertEquals(retrievedSummary.getId(), summary1.getId());
        assertEquals(retrievedSummary.getName(), summary1.getName());

        List<Predictor> predictors = retrievedSummary.getPredictors();
        assertEquals(predictors.size(), 3);

        KeyValue details = retrievedSummary.getDetails();
        String uncompressedStr = new String(CompressionUtils.decompressByteArray(details.getData()));
        assertEquals(details.getTenantId(), summary1.getTenantId());
        assertTrue(uncompressedStr.contains("\"Segmentations\":"));

        String[] predictorFields = new String[] { "name", //
                "displayName", //
                "approvedUsage", //
                "category", //
                "fundamentalType", //
                "uncertaintyCoefficient", //
                "usedForBuyerInsights", //
                "tenantId" };

        String[] predictorElementFields = new String[] { "name", //
                "correlationSign", //
                "count", //
                "lift", //
                "lowerInclusive", //
                "upperExclusive", //
                "uncertaintyCoefficient", //
                "visible" };

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
        setupSecurityContext(summary2);
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
        assertEquals(summaries.size(), 1);
        assertEquals(summaries.get(0).getName(), summary2.getName());
    }

    @Test(groups = "functional", dependsOnMethods = { "findByModelId", "findAll" })
    public void updateModelSummaryForModelInTenant() {
        setupSecurityContext(summary1);
        ModelSummary s = modelSummaryEntityMgr.findValidByModelId(summary1.getId());
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("DisplayName", "XYZ");
        modelSummaryEntityMgr.updateModelSummary(s, attrMap);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.findValidByModelId(summary1.getId());
        assertEquals(retrievedSummary.getDisplayName(), "XYZ");
    }

    /**
     * Update summary from tenant 2 but using tenant 1 security context.
     */
    @Test(groups = "functional", dependsOnMethods = { "updateModelSummaryForModelInTenant" })
    public void updateModelSummaryForModelNotInTenant() {
        ModelSummary summaryToUpdate = new ModelSummary();
        summaryToUpdate.setId(summary2.getId());
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("DisplayName", "ABC");

        setupSecurityContext(summary1);
        boolean exception = false;
        try {
            modelSummaryEntityMgr.updateModelSummary(summaryToUpdate, attrMap);
        } catch (LedpException e) {
            exception = true;
            assertEquals(e.getCode(), LedpCode.LEDP_18007);
        }
        assertTrue(exception);
    }

    @Test(groups = "functional", dependsOnMethods = { "updateModelSummaryForModelNotInTenant" })
    public void updateAsDeletedForActiveModel() {
        setupSecurityContext(summary1);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.findValidByModelId(summary1.getId());
        assertNotNull(retrievedSummary);
        try {
            modelSummaryEntityMgr.updateStatusByModelId(summary1.getId(), ModelSummaryStatus.ACTIVE);
            modelSummaryEntityMgr.updateStatusByModelId(summary1.getId(), ModelSummaryStatus.DELETED);
            Assert.fail("Should not come here!");
        } catch (LedpException ex) {
            Assert.assertEquals(ex.getCode(), LedpCode.LEDP_18021);
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "updateAsDeletedForActiveModel" })
    public void updateAsDeletedForInactiveModel() {
        setupSecurityContext(summary1);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.findValidByModelId(summary1.getId());
        assertNotNull(retrievedSummary);
        retrievedSummary.setStatus(ModelSummaryStatus.INACTIVE);
        modelSummaryEntityMgr.update(retrievedSummary);
        modelSummaryEntityMgr.updateStatusByModelId(summary1.getId(), ModelSummaryStatus.DELETED);
        assertNotNull(modelSummaryEntityMgr.findByModelId(summary1.getId(), true, true, false));
        assertEquals(modelSummaryEntityMgr.findByModelId(summary1.getId(), true, true, false).getStatus(),
                ModelSummaryStatus.DELETED);
        List<ModelSummary> modelSummaryList = modelSummaryEntityMgr.findAllValid();
        Assert.assertEquals(modelSummaryList.size(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "updateAsDeletedForInactiveModel" })
    public void updateAsActiveForDeletedModel() {
        setupSecurityContext(summary1);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.getByModelId(summary1.getId());
        assertNotNull(retrievedSummary);
        try {
            modelSummaryEntityMgr.updateStatusByModelId(summary1.getId(), ModelSummaryStatus.ACTIVE);
            Assert.fail("Should not come here!");
        } catch (LedpException ex) {
            Assert.assertEquals(ex.getCode(), LedpCode.LEDP_18024);
        }

        modelSummaryEntityMgr.updateStatusByModelId(summary1.getId(), ModelSummaryStatus.INACTIVE);
        modelSummaryEntityMgr.updateStatusByModelId(summary1.getId(), ModelSummaryStatus.ACTIVE);

    }

    @Test(groups = "functional", dependsOnMethods = { "updateAsActiveForDeletedModel" })
    public void deleteForModelInTenant() {
        setupSecurityContext(summary1);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.findValidByModelId(summary1.getId());
        assertNotNull(retrievedSummary);
        modelSummaryEntityMgr.deleteByModelId(summary1.getId());
        assertNull(modelSummaryEntityMgr.findValidByModelId(summary1.getId()));
        KeyValue kv = new KeyValue();
        kv.setPid(retrievedSummary.getDetails().getPid());
        assertNull(keyValueEntityMgr.findByKey(kv));
    }

    @Test(groups = "functional", dependsOnMethods = { "deleteForModelInTenant" })
    public void deleteForModelNotInTenant() {
        setupSecurityContext(summary1);
        boolean exception = false;
        try {
            modelSummaryEntityMgr.deleteByModelId(summary1.getId());
        } catch (LedpException e) {
            exception = true;
            assertEquals(e.getCode(), LedpCode.LEDP_18007);
        }
        assertTrue(exception);
    }

    @Test(groups = "functional")
    public void testFindAndUpdatePredictorsForSummary() {
        setupSecurityContext(summary1);
        ModelSummary retrievedSummary = modelSummaryEntityMgr.findByModelId(summary1.getId(), true, false, false);

        List<Predictor> predictorsUsedForBi = modelSummaryEntityMgr.findPredictorsUsedByBuyerInsightsByModelId(summary1
                .getId());
        assertTrue(predictorsUsedForBi.size() == 2);

        List<Predictor> predictors = retrievedSummary.getPredictors();
        AttributeMap attrMap = createValidMap();
        modelSummaryEntityMgr.updatePredictors(predictors, attrMap);

        ModelSummary retrievedSummaryAfterUpdatingPredictors = modelSummaryEntityMgr.findByModelId(summary1.getId(),
                true, false, false);

        predictorsUsedForBi = modelSummaryEntityMgr.findPredictorsUsedByBuyerInsightsByModelId(summary1.getId());
        assertTrue(predictorsUsedForBi.size() == 1);

        predictors = retrievedSummaryAfterUpdatingPredictors.getPredictors();
        for (Predictor predictor : predictors) {
            String predictorName = predictor.getName();
            switch (predictorName) {
            case "LeadSource":
                assertTrue(predictor.getUsedForBuyerInsights() == false);
                break;
            case "Website_Custom":
                assertTrue(predictor.getUsedForBuyerInsights() == false);
                break;
            case "Income":
                assertTrue(predictor.getUsedForBuyerInsights() == true);
                break;
            }
        }

        attrMap = createInvalidMap();
        try {
            modelSummaryEntityMgr.updatePredictors(predictors, attrMap);
            assertTrue(true, "Should have thrown exception.");
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertTrue(((LedpException) e).getCode().equals(LedpCode.LEDP_18052));
        }
    }

    private AttributeMap createValidMap() {
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("LeadSource", "0");
        attrMap.put("Website_Custom", "0");
        attrMap.put("Income", "1");
        return attrMap;
    }

    private AttributeMap createInvalidMap() {
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("LeadSource", "0");
        attrMap.put("Browser", "0");
        return attrMap;
    }
}
