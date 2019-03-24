package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class PropDataEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    private PropData propData;
    private final String propDataName = "PropDataEntityMgrImplTestNG";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        PropData alreadyExists = propDataEntityMgr.findByName(propDataName);
        if (alreadyExists != null)
            propDataEntityMgr.delete(alreadyExists);
        propData = new PropData();
        propData.setName(propDataName);
        // Doesn't require DataCloud 2.0.14 dynamo table really exists
        propData.setDataCloudVersion("2.0.1470268608");
    }

    @Override
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        propDataEntityMgr.delete(propData);
        super.tearDown();
    }

    @Test(groups = "functional")
    public void create() {
        propDataEntityMgr.create(propData);

        List<PropData> propDatas = propDataEntityMgr.findAll();
        assertNotNull(propDatas);

        PropData retrievedPropData = propDataEntityMgr.findByName("PropDataEntityMgrImplTestNG");
        assertEquals(retrievedPropData.getName(), propData.getName());
        assertEquals(retrievedPropData.getDataCloudVersion(), propData.getDataCloudVersion());
    }
}
