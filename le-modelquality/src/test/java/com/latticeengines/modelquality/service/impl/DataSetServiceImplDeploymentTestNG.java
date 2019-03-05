package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenance;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.modelquality.functionalframework.ModelQualityTestNGBase;
import com.latticeengines.modelquality.service.DataSetService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

public class DataSetServiceImplDeploymentTestNG extends ModelQualityTestNGBase {

    @Inject
    private DataSetService dataSetService;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @BeforeClass(groups = "deployment")
    public void setup() {
    }

    @AfterClass(groups = "deployment")
    public void tearDown() {
    }

    @Test(groups = "deployment", enabled = true)
    public void createDataSetFromTenant() {
        String tenantId = getTestTenant();
        String modelId = getTestModelID();
        ModelSummary spiedModelSummary = spy(new ModelSummary());
        doReturn("SalesforceLead").when(spiedModelSummary).getSourceSchemaInterpretation();
        ModelSummaryProvenance spiedModelSummaryProvenance = spy(new ModelSummaryProvenance());
        doReturn("/some/hdfs/path").when(spiedModelSummaryProvenance).getString(ProvenancePropertyName.TrainingFilePath,
                "");
        doReturn(spiedModelSummaryProvenance).when(spiedModelSummary).getModelSummaryConfiguration();

        modelSummaryProxy = spy(modelSummaryProxy);
        doReturn(spiedModelSummary).when(modelSummaryProxy).getModelSummaryFromModelId(tenantId, modelId);

        DataSetServiceImpl spiedDataSetService = spy(((DataSetServiceImpl) dataSetService));
        spiedDataSetService.modelSummaryProxy = modelSummaryProxy;
        String dataSetName = spiedDataSetService.createDataSetFromLP2Tenant(tenantId, modelId);
        Assert.assertNotNull(dataSetName);

        String dataSetName1 = spiedDataSetService.createDataSetFromLP2Tenant(tenantId, modelId);
        Assert.assertNotNull(dataSetName, dataSetName1);

        if (dataSetName != null && !dataSetName.isEmpty()) {
            dataSetEntityMgr.delete(dataSetEntityMgr.findByName(dataSetName));
        }
    }

    private String getTestModelID() {
        return "ms__e63db730-ab56-4185-a970-1ccae324a792-SelfServ";
    }

    private String getTestTenant() {
        return "LETest1478910868849.LETest1478910868849.Production";
    }
}
