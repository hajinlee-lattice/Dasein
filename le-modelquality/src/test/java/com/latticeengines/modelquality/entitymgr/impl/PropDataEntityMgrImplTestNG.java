package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class PropDataEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    private PropData propData;

    @Autowired
    private PropDataEntityMgr propDataEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        propDataEntityMgr.deleteAll();

        propData = new PropData();
        propData = new PropData();
        propData.setVersion("");
        propData.setMetadataVersion("MetadataVersion1");
    }

    @Test(groups = "functional")
    public void create() {
        propDataEntityMgr.create(propData);

        List<PropData> propDatas = propDataEntityMgr.findAll();
        assertEquals(propDatas.size(), 1);
        PropData retrievedPropData = propDatas.get(0);

        assertEquals(retrievedPropData.getName(), propData.getName());
        assertEquals(retrievedPropData.getVersion(), propData.getVersion());
        assertEquals(retrievedPropData.getMetadataVersion(), propData.getMetadataVersion());
    }
}
