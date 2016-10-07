package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class DataFlowEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    private DataFlow dataFlow;

    @Autowired
    private DataFlowEntityMgr dataFlowEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        cleanupDb();

        dataFlow = new DataFlow();
        dataFlow.setName("DataFlow1");
        dataFlow.setMatch(Boolean.TRUE);
        dataFlow.setTransformationGroup(TransformationGroup.STANDARD);
    }

    @Test(groups = "functional")
    public void create() {
        dataFlowEntityMgr.create(dataFlow);

        List<DataFlow> dataFlows = dataFlowEntityMgr.findAll();
        assertEquals(dataFlows.size(), 1);
        DataFlow retrievedDataFlow = dataFlows.get(0);

        assertEquals(retrievedDataFlow.getName(), dataFlow.getName());
        assertEquals(retrievedDataFlow.getMatch(), Boolean.TRUE);
        assertEquals(retrievedDataFlow.getTransformationGroup(), TransformationGroup.STANDARD);
    }
}
