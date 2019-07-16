package com.latticeengines.apps.lp.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.service.ModelSummaryCacheService;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.pls.EntityListCache;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;

public class ModelSummaryCacheServiceImplTestNG extends LPFunctionalTestNGBase {

    @Autowired
    private ModelSummaryCacheService modelSummaryCacheService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    private Tenant tenant1;
    private Tenant tenant2;

    private List<ModelSummary> modelSummaries1;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        testBed.bootstrap(2);
        tenant1 = testBed.getMainTestTenant();
        tenant2 = testBed.getTestTenants().get(1);
        modelSummaries1 = createModelSummariesForTenant(tenant1);
        createModelSummariesForTenant(tenant2);
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        deleteIdsAndEntitiesByTenant(tenant1);
        deleteIdsAndEntitiesByTenant(tenant2);
        assertTrue(getEntitiesByTenant(tenant1).isEmpty());
        assertTrue(getEntitiesByTenant(tenant2).isEmpty());
    }

    private void deleteIdsAndEntitiesByTenant(Tenant tenant) {
        modelSummaryCacheService.deleteIdsAndEntitiesByTenant(tenant);
    }

    private List<ModelSummary> getEntitiesByTenant(Tenant tenant) {
        return modelSummaryCacheService.getEntitiesByTenant(tenant);
    }

    @Test(groups = {"functional"})
    public void testBuildEntitiesCache() {
        try {
            verifyBuildEntitiesCache(tenant1);
            List<String> ids =
                    modelSummaries1.stream().map(modelSummary -> modelSummary.getId()).collect(Collectors.toList());
            // some entities don't exist
            modelSummaryCacheService.deleteEntitiesByIds(ids.subList(0, 4));

            verifyBuildEntitiesCache(tenant1);

            modelSummaryCacheService.deleteIdsByTenant(tenant1);
            modelSummaryCacheService.deleteEntitiesByIds(ids.subList(0, 4));
            verifyBuildEntitiesCache(tenant1);

            verifyClearModelSummaryCache(tenant1, modelSummaries1.get(0).getId(), ids.subList(1, ids.size()), 9);
            verifyClearModelSummaryCache(tenant1, modelSummaries1.get(1).getId(), ids.subList(2, ids.size()), 8);
            verifyBuildEntitiesCache(tenant1);

            verifyBuildEntitiesCache(tenant2);
        } catch (Exception e) {
            Assert.fail("Exception should not happen");
        }
    }

    private void verifyClearModelSummaryCache(Tenant tenant, String id, List<String> ids, int size) throws Exception {
        Future<?> future = modelSummaryCacheService.clearCache(tenant, id);
        future.get();
        EntityListCache entityListCache = modelSummaryCacheService.getEntitiesAndNonExistEntitityIdsByTenant(tenant);
        assertEquals(entityListCache.getExistEntities().size(), 0);
        assertEquals(entityListCache.getNonExistIds().size(), 0);
        assertEquals(modelSummaryCacheService.getEntitiesByIds(ids).size(), size);
    }

    private void verifyBuildEntitiesCache(Tenant tenant) throws Exception {
        Future<?> future = modelSummaryCacheService.buildEntitiesCache(tenant);
        future.get();
        List<ModelSummary> modelSummaries = modelSummaryCacheService.getEntitiesByTenant(tenant);
        assertEquals(modelSummaries.size(), 10);
        assertEquals(modelSummaries.get(0).getTenantId(), tenant.getPid());
    }

    private List<ModelSummary> createModelSummariesForTenant(Tenant tenant) throws Exception {
        List<ModelSummary> modelSummaries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ModelSummary summary = new ModelSummary();
            String id = UUID.randomUUID().toString();
            summary.setId(id);
            summary.setName("Model1" + id);
            summary.setRocScore(0.75);
            summary.setLookupId(
                    String.format("%s|Q_EventTable_%s|abcde", tenant.getName(), tenant.getName()));
            summary.setTrainingRowCount(8000L);
            summary.setTestRowCount(2000L);
            summary.setTotalRowCount(10000L);
            summary.setTrainingConversionCount(80L);
            summary.setTestConversionCount(20L);
            summary.setTotalConversionCount(100L);
            summary.setConstructionTime(System.currentTimeMillis());
            if (summary.getConstructionTime() == null) {
                summary.setConstructionTime(System.currentTimeMillis());
            }
            summary.setLastUpdateTime(summary.getConstructionTime());
            summary.setTenant(tenant);
            setDetails(summary);
            Predictor s1p1 = new Predictor();
            s1p1.setApprovedUsage("Model");
            s1p1.setCategory("Banking");
            s1p1.setName("LeadSource");
            s1p1.setDisplayName("LeadSource");
            s1p1.setFundamentalType("");
            s1p1.setUncertaintyCoefficient(0.151911);
            s1p1.setUsedForBuyerInsights(true);
            summary.addPredictor(s1p1);
            Predictor s1p2 = new Predictor();
            s1p2.setApprovedUsage("ModelAndModelInsights");
            s1p2.setCategory("Banking");
            s1p2.setName("Website_Custom");
            s1p2.setDisplayName("Website_Custom");
            s1p2.setFundamentalType("");
            s1p2.setUncertaintyCoefficient(0.251911);
            s1p2.setUsedForBuyerInsights(true);
            summary.addPredictor(s1p2);
            Predictor s1p3 = new Predictor();
            s1p3.setApprovedUsage("ModelAndModelInsights");
            s1p3.setCategory("Finance");
            s1p3.setName("Income");
            s1p3.setDisplayName("Income");
            s1p3.setFundamentalType("numeric");
            s1p3.setUncertaintyCoefficient(0.171911);
            s1p3.setUsedForBuyerInsights(false);
            summary.addPredictor(s1p3);
            summary.setModelType(ModelType.PYTHONMODEL.getModelType());

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
            modelSummaryEntityMgr.create(summary);
            modelSummaries.add(summary);
        }
        return modelSummaries;
    }

    private void setDetails(ModelSummary summary) throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader
                .getSystemResourceAsStream("modelsummary/modelsummary-marketo.json");
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        details.setData(data);
        summary.setDetails(details);
    }

}
