package com.latticeengines.modelquality.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummaryMetrics;
import com.latticeengines.modelquality.entitymgr.ModelSummaryMetricsEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class ModelSummaryMetricsServiceImplTestNG extends ModelQualityFunctionalTestNGBase {

    private String tenantName;

    @Autowired
    private ModelSummaryMetricsEntityMgr modelSummaryMetricsEntityMgr;

    private ModelSummaryMetrics summary1;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenantName = "TENANT1";
        summary1 = createModelSummaryMetricsForTenant1();
        displayModelSummaryMetricsForTenant1();
        deleteModelSummaryMetricsForTenant1();
    }

    public ModelSummaryMetrics createModelSummaryMetrics(ModelSummaryMetrics modelSummary, String tenantName) {
        modelSummary.setName(tenantName);

        if (modelSummary.getConstructionTime() == null) {
            modelSummary.setConstructionTime(System.currentTimeMillis());
        }
        modelSummary.setLastUpdateTime(modelSummary.getConstructionTime());
        modelSummaryMetricsEntityMgr.create(modelSummary);

        return modelSummary;
    }

    public void updateLastUpdateTime(String modelId) {
        ModelSummaryMetrics modelSummary = modelSummaryMetricsEntityMgr.getByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }
        modelSummaryMetricsEntityMgr.updateLastUpdateTime(modelSummary);
    }

    public void deleteModelSummaryMetrics(ModelSummaryMetrics modelSummaryMetrics) {
        modelSummaryMetricsEntityMgr.delete(modelSummaryMetrics);
    }

    public ModelSummaryMetrics getModelSummaryMetricsById(String id) {
        return modelSummaryMetricsEntityMgr.getByModelId(id);
    }

    private ModelSummaryMetrics createModelSummaryMetricsForTenant1() throws Exception {
        tenantName = "TENANT1";
        summary1 = new ModelSummaryMetrics();
        summary1.setId("123");
        summary1.setName("Model1");
        summary1.setRocScore(0.75);
        summary1.setTop20PercentLift(4.34);
        summary1.setConstructionTime(System.currentTimeMillis());
        if (summary1.getConstructionTime() == null) {
            summary1.setConstructionTime(System.currentTimeMillis());
        }
        summary1.setLastUpdateTime(summary1.getConstructionTime());
        summary1.setName(tenantName);

        createModelSummaryMetrics(summary1, tenantName);
        return summary1;
    }

    private void displayModelSummaryMetricsForTenant1() {
        ModelSummaryMetrics modelSummaryMetrics = getModelSummaryMetricsById("123");
        Assert.assertEquals(modelSummaryMetrics.getName(), "Model1");
        Assert.assertEquals(modelSummaryMetrics.getRocScore(), 0.75);
        Assert.assertEquals(modelSummaryMetrics.getTop20PercentLift(), 4.34);
        Assert.assertNotNull(modelSummaryMetrics.getConstructionTime());
        Assert.assertNotNull(modelSummaryMetrics.getLastUpdateTime());
        Assert.assertNotNull(modelSummaryMetrics.getTenantId());
    }

    private void deleteModelSummaryMetricsForTenant1() {
        ModelSummaryMetrics modelSummaryMetrics = getModelSummaryMetricsById("123");
        deleteModelSummaryMetrics(modelSummaryMetrics);
    }

}
