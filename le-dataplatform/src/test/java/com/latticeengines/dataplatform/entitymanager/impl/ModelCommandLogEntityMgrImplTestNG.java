package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

public class ModelCommandLogEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelCommandLogEntityMgr modelCommandLogEntityMgr;

    @Autowired
    private ModelCommandLogService modelCommandLogService;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "functional")
    public void testFindByModelCommand() throws Exception {
        ModelCommand modelCommand = new ModelCommand(1L, "Nutanix", "Nutanix", ModelCommandStatus.NEW, null,
                ModelCommand.TAHOE, ModelingServiceTestUtils.EVENT_TABLE);
        modelCommandEntityMgr.create(modelCommand);

        ModelCommand anotherModelCommand = new ModelCommand(2L, "Nutanix", "Nutanix", ModelCommandStatus.NEW,
                null, ModelCommand.TAHOE, ModelingServiceTestUtils.EVENT_TABLE);
        modelCommandEntityMgr.create(anotherModelCommand);

        for (int i = 0; i < 3; i++) {
            modelCommandLogService.log(modelCommand, "a message" + i);
        }

        modelCommandLogService.log(anotherModelCommand, "a message");

        assertEquals(modelCommandLogEntityMgr.findByModelCommand(modelCommand).size(), 3);
        assertEquals(modelCommandLogEntityMgr.findByModelCommand(anotherModelCommand).size(), 1);
    }

}
