package com.latticeengines.apps.lp.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.lp.service.ModelSummaryCacheService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.db.exposed.entitymgr.KeyValueEntityMgr;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.EntityListCache;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.security.exposed.service.TenantService;

public class ModelSummaryServiceImplTestNG extends LPFunctionalTestNGBase {

    @InjectMocks
    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private ModelSummaryCacheService modelSummaryCacheService;

    @Spy
    @Inject
    private AttrConfigService lpAttrConfigService;

    @Inject
    private KeyValueEntityMgr keyValueEntityMgr;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private TenantService tenantService;

    private ModelSummary summary1;

    private List<AttrConfig> attrs;

    private Tenant tenant1;

    private String editedDisplayName = "Industry Rollup edited";

    private List<ModelSummary> allModelSummaries;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        tenant1 = tenantService.findByTenantId("TENANT1");

        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        MultiTenantContext.setTenant(tenant1);
        summary1 = createModelSummaryForTenant1(tenant1);
        allModelSummaries = createModelSummariesForTenant(tenant1);
        allModelSummaries.add(summary1);
        List<AttrConfig> attrConfigs = attrConfigEntityMgr.findAllForEntity(tenant1.getId(), BusinessEntity.Account);
        if (CollectionUtils.isNotEmpty(attrConfigs)) {
            attrConfigEntityMgr.deleteAllForEntity(tenant1.getId(), BusinessEntity.Account);
            Thread.sleep(500); // wait for replication lag
        }

        AttrConfig attrConfig1 = new AttrConfig();
        attrConfig1.setAttrName("Industry_Group");
        AttrConfigProp<String> attrConfigProp1 = new AttrConfigProp<>();
        attrConfigProp1.setCustomValue(editedDisplayName);
        attrConfig1.putProperty(ColumnMetadataKey.DisplayName, attrConfigProp1);
        attrConfigEntityMgr.save(tenant1.getId(), BusinessEntity.Account, Arrays.asList(attrConfig1));
        Thread.sleep(500); // wait for replication lag
        attrs = attrConfigEntityMgr.findAllHaveCustomDisplayNameByTenantId(tenant1.getId());
        Assert.assertEquals(attrs.size(), 1);
        attrs.get(0).getProperty(ColumnMetadataKey.DisplayName).setAllowCustomization(true);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        tenant1 = tenantService.findByTenantId("TENANT1");
        tenantService.discardTenant(tenant1);

        attrConfigEntityMgr.deleteAllForEntity(tenant1.getId(), BusinessEntity.Account);
        modelSummaryCacheService.deleteIdsAndEntitiesByTenant(tenant1);
        assertTrue(modelSummaryCacheService.getEntitiesByTenant(tenant1).isEmpty());
    }

    private void setDetails(ModelSummary summary) throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader
                .getSystemResourceAsStream("modelsummary/modelsummary-marketo-UI-issue.json");
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        details.setData(data);
        summary.setDetails(details);
    }

    private ModelSummary createModelSummaryForTenant1(Tenant tenant) throws Exception {
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setId(UUID.randomUUID().toString());
        modelSummary.setName("Model1");
        modelSummary.setRocScore(0.75);
        modelSummary.setLookupId("TENANT1|Q_EventTable_TENANT1|abcde");
        modelSummary.setTrainingRowCount(8000L);
        modelSummary.setTestRowCount(2000L);
        modelSummary.setTotalRowCount(10000L);
        modelSummary.setTrainingConversionCount(80L);
        modelSummary.setTestConversionCount(20L);
        modelSummary.setTotalConversionCount(100L);
        modelSummary.setConstructionTime(System.currentTimeMillis());
        modelSummary.setSourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.toString());
        if (modelSummary.getConstructionTime() == null) {
            modelSummary.setConstructionTime(System.currentTimeMillis());
        }
        modelSummary.setLastUpdateTime(modelSummary.getConstructionTime());
        modelSummary.setTenant(tenant1);
        setDetails(modelSummary);
        Predictor s1p1 = new Predictor();
        s1p1.setApprovedUsage("Model");
        s1p1.setCategory("Banking");
        s1p1.setName("LeadSource");
        s1p1.setDisplayName("LeadSource");
        s1p1.setFundamentalType("");
        s1p1.setUncertaintyCoefficient(0.151911);
        s1p1.setUsedForBuyerInsights(true);
        modelSummary.addPredictor(s1p1);
        Predictor s1p2 = new Predictor();
        s1p2.setApprovedUsage("ModelAndModelInsights");
        s1p2.setCategory(Category.LEAD_INFORMATION.getName());
        s1p2.setName("Website_Custom");
        s1p2.setDisplayName("Website_Custom");
        s1p2.setFundamentalType("");
        s1p2.setUncertaintyCoefficient(0.251911);
        s1p2.setUsedForBuyerInsights(true);
        modelSummary.addPredictor(s1p2);
        Predictor s1p3 = new Predictor();
        s1p3.setApprovedUsage("ModelAndModelInsights");
        s1p3.setCategory("Finance");
        s1p3.setName("Income");
        s1p3.setDisplayName("Income");
        s1p3.setFundamentalType("numeric");
        s1p3.setUncertaintyCoefficient(0.171911);
        s1p3.setUsedForBuyerInsights(false);
        modelSummary.addPredictor(s1p3);
        modelSummary.setModelType(ModelType.PYTHONMODEL.getModelType());

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

        modelSummaryService.createModelSummary(modelSummary, tenant.getId());
        return modelSummary;
    }

    @Test(groups = "functional")
    public void findByModelId() throws IOException, ExecutionException, InterruptedException {
        ModelSummary retrievedSummary = modelSummaryService.getModelSummary(summary1.getId());
        assertEquals(retrievedSummary.getId(), summary1.getId());
        assertEquals(retrievedSummary.getName(), summary1.getName());
        long oldLastUpdateTime = retrievedSummary.getLastUpdateTime();
        modelSummaryService.updateLastUpdateTime(summary1.getId());
        long newLastUpdateTime = modelSummaryService.getModelSummary(summary1.getId()).getLastUpdateTime();
        assertTrue(newLastUpdateTime > oldLastUpdateTime);

        KeyValue keyValue = retrievedSummary.getDetails();
        String uncompressedStr = new String(CompressionUtils.decompressByteArray(keyValue.getData()));
        assertEquals(keyValue.getTenantId(), summary1.getTenantId());
        assertTrue(uncompressedStr.contains("\"Segmentations\":"));
        assertTrue(
                uncompressedStr.contains(String.format("\"%s\":true", ModelSummaryServiceImpl.REVENUE_UI_ISSUE_FIXED)));
        assertTrue(uncompressedStr
                .contains(String.format("\"%s\":true", ModelSummaryServiceImpl.ACCOUNT_CATEGORY_ISSUE_FIXED)));
        assertTrue(uncompressedStr.equals(keyValueEntityMgr.findByTenantId(tenant1.getPid()).get(0).getPayload()));

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode details = objectMapper.readTree(keyValue.getPayload());
        ArrayNode predictors = (ArrayNode) details.get("Predictors");
        for (JsonNode predictor : predictors) {
            assertTrue(!predictor.get("Name").asText().equals("WebMasterRegistrationsTopAttributes"));
            if (predictor.get("Name").asText().equals(ModelSummaryServiceImpl.BUSINESS_ANNUAL_SALES_ABS)) {
                testFixBusinessAnnualSalesAbs(predictor);
            } else if (predictor.get("Name").asText().equals("Website_Custom")) {
                testFixAccountCategory(predictor);
            } else if (predictor.get("Name").asText().equals("Industry_Group")) {
                testFixCustomDisplayNames(predictor);
            }
        }
        retrievedSummary = modelSummaryService.getModelSummary(summary1.getId());
        KeyValue keyValue2 = retrievedSummary.getDetails();
        String uncompressedStr2 = new String(CompressionUtils.decompressByteArray(keyValue2.getData()));
        assertEquals(uncompressedStr, uncompressedStr2);
        modelSummaryCacheService.setIdsAndEntitiesByTenant(tenant1, allModelSummaries);
        List<ModelSummary> modelSummaries = modelSummaryService.getModelSummaries("all");
        assertEquals(modelSummaries.size(), 6);
        Future<?> future = modelSummaryCacheService.clearModelSummaryCache(tenant1, allModelSummaries.get(0).getId());
        future.get();
        EntityListCache entityListCache = modelSummaryCacheService.getEntitiesAndNonExistEntitityIdsByTenant(tenant1);
        assertEquals(entityListCache.getNonExistIds().size(), 0);
        assertEquals(entityListCache.getExistEntities().size(), 0);
        modelSummaries = modelSummaryService.getModelSummaries("all");
        assertEquals(modelSummaries.size(), 6);
        List<String> ids = allModelSummaries.stream().map(ModelSummary::getId).collect(Collectors.toList());
        int time = 3;
        // make sure build cache process is completed
        while (modelSummaryCacheService.getEntitiesByIds(ids).size() != 6 && time > 0) {
            time--;
            Thread.sleep(500);
        }
        ids = ids.subList(0, 3);
        modelSummaryCacheService.deleteEntitiesByIds(ids);
        entityListCache = modelSummaryCacheService.getEntitiesAndNonExistEntitityIdsByTenant(tenant1);
        assertEquals(entityListCache.getNonExistIds().size(), 3);
        assertEquals(entityListCache.getExistEntities().size(), 3);
        modelSummaries = modelSummaryService.getModelSummaries("all");
        assertEquals(modelSummaries.size(), 6);
    }

    private void testFixCustomDisplayNames(JsonNode predictor) {
        Assert.assertEquals(predictor.get(ColumnMetadataKey.DisplayName).asText(), editedDisplayName);
    }

    private void testFixBusinessAnnualSalesAbs(JsonNode predictor) {
        ArrayNode elements = (ArrayNode) predictor.get(ModelSummaryServiceImpl.ELEMENTS);
        for (JsonNode element : elements) {
            if (element.get(ModelSummaryServiceImpl.LOWER_INCLUSIVE).asText() != null) {
                assertTrue(element.get(ModelSummaryServiceImpl.LOWER_INCLUSIVE).asLong() == 0
                        || element.get(ModelSummaryServiceImpl.LOWER_INCLUSIVE).asLong() > 20000000);
            }
            if (element.get(ModelSummaryServiceImpl.UPPER_EXCLUSIVE).asText() != null) {
                assertTrue(element.get(ModelSummaryServiceImpl.UPPER_EXCLUSIVE).asLong() == 0
                        || element.get(ModelSummaryServiceImpl.UPPER_EXCLUSIVE).asLong() > 20000000);
            }
        }
    }

    private void testFixAccountCategory(JsonNode predictor) {
        String category = predictor.get(ModelSummaryServiceImpl.CATEGORY).asText();
        assertEquals(category, Category.ACCOUNT_INFORMATION.getName());
    }

    private List<ModelSummary> createModelSummariesForTenant(Tenant tenant) throws Exception {
        List<ModelSummary> modelSummaries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            modelSummaries.add(createModelSummaryForTenant1(tenant));
        }
        return modelSummaries;
    }
}
