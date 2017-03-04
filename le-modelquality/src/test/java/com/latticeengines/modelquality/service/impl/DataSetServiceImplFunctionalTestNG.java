package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenance;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.DataSetService;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class DataSetServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private DataSetService dataSetService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "functional", enabled = true)
    public void createDataSetFromTenant() {
        String tenantId = getTestTenant();
        String modelId = getTestModelID();
        ModelSummary spiedModelSummary = spy(new ModelSummary());
        doReturn("SalesforceLead").when(spiedModelSummary).getSourceSchemaInterpretation();
        ModelSummaryProvenance spiedModelSummaryProvenance = spy(new ModelSummaryProvenance());
        doReturn("/some/hdfs/path").when(spiedModelSummaryProvenance).getString(ProvenancePropertyName.TrainingFilePath,
                "");
        doReturn(spiedModelSummaryProvenance).when(spiedModelSummary).getModelSummaryConfiguration();

        InternalResourceRestApiProxy internalResourceRestApiProxy = spy(new InternalResourceRestApiProxy(null));
        doReturn(spiedModelSummary).when(internalResourceRestApiProxy).getModelSummaryFromModelId(modelId,
                CustomerSpace.parse(tenantId));

        DataSetServiceImpl spiedDataSetService = spy(((DataSetServiceImpl) dataSetService));
        spiedDataSetService.internalResourceRestApiProxy = internalResourceRestApiProxy;
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
