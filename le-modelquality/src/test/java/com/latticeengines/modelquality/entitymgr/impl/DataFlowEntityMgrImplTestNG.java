package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class DataFlowEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    private DataFlow dataFlow;
    private final String dataFlowName = "DataFlowEntityMgrImplTestNG";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        DataFlow alreadyExists = dataFlowEntityMgr.findByName(dataFlowName);
        if (alreadyExists != null)
            dataFlowEntityMgr.delete(alreadyExists);
        dataFlow = new DataFlow();
        dataFlow.setName(dataFlowName);
        dataFlow.setMatch(Boolean.TRUE);
        dataFlow.setTransformationGroup(TransformationGroup.STANDARD);
    }

    @Override
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        dataFlowEntityMgr.delete(dataFlow);
        super.tearDown();
    }

    @Test(groups = "functional")
    public void create() {
        dataFlowEntityMgr.create(dataFlow);

        List<DataFlow> dataFlows = dataFlowEntityMgr.findAll();
        assertNotNull(dataFlows);

        DataFlow retrievedDataFlow = dataFlowEntityMgr.findByName("DataFlowEntityMgrImplTestNG");
        assertEquals(retrievedDataFlow.getName(), dataFlow.getName());
        assertEquals(retrievedDataFlow.getMatch(), Boolean.TRUE);
        assertEquals(retrievedDataFlow.getTransformationGroup(), TransformationGroup.STANDARD);
    }
}
