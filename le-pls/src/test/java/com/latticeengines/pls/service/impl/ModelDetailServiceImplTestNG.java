package com.latticeengines.pls.service.impl;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.CategoryObject;
import com.latticeengines.domain.exposed.pls.ModelDetail;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.ModelDetailService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class ModelDetailServiceImplTestNG extends PlsFunctionalTestNGBase {
    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private ModelDetailService modelDetailService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;

    private ModelSummary summary1;

    private Tenant tenant1;

    private final String tenantName = ModelDetailServiceImpl.class.getName();
    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenant1 = tenantService.findByTenantId(tenantName);

        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }

        summary1 = createModelSummaryForTenant1();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        tenant1 = tenantService.findByTenantId(tenantName);
        tenantService.discardTenant(tenant1);
    }

    private void setDetails(ModelSummary summary) throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/pls/functionalframework/modelsummary-marketo-UI-issue.json");
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        details.setData(data);
        summary.setDetails(details);
    }

    private ModelSummary createModelSummaryForTenant1() throws Exception {
        tenant1 = new Tenant();
        tenant1.setId(tenantName);
        tenant1.setName(tenantName);
        tenantEntityMgr.create(tenant1);

        MultiTenantContext.setTenant(tenant1);
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
        summary1.setSourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.toString());
        if (summary1.getConstructionTime() == null) {
            summary1.setConstructionTime(System.currentTimeMillis());
        }
        summary1.setLastUpdateTime(summary1.getConstructionTime());
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
        s1p2.setCategory(Category.LEAD_INFORMATION.getName());
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
        summary1.setModelType(ModelType.PYTHONMODEL.getModelType());

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

        modelSummaryService.createModelSummary(summary1, tenant1.getId());
        return summary1;
    }

    @Test(groups = "functional")
    private void testGetModelDetail() {
        ModelSummary testSummary = modelSummaryService.getModelSummary(summary1.getId());
        ModelDetail modelDetail = modelDetailService.getModelDetail(testSummary.getId());
        assertNotNull(modelDetail);
        Map<String, CategoryObject> details = modelDetail.getCategories();
        assertNotNull(details);
        CategoryObject attribute = details.get("Technologies");
        if (attribute != null) {
            Boolean isInternal = attribute.getIsInternal();
            assertEquals(isInternal, Boolean.FALSE);
        }
    }
}
