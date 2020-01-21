package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Date;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
public class ModelCommandResultEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Inject
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Inject
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "functional", enabled = false)
    public void testFindByModelCommand() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L);
        modelCommandEntityMgr.create(command);

        assertNull(modelCommandResultEntityMgr.findByModelCommand(command));

        Date now = new Date();
        ModelCommandResult result = new ModelCommandResult(command, now, now, ModelCommandStatus.IN_PROGRESS);
        modelCommandResultEntityMgr.create(result);

        ModelCommandResult retrieved = modelCommandResultEntityMgr.findByModelCommand(command);
        assertNotNull(retrieved);
        assertEquals(retrieved.getModelCommand(), command);

        retrieved.setEndTime(new Date());
        retrieved.setProcessStatus(ModelCommandStatus.SUCCESS);
        modelCommandResultEntityMgr.update(retrieved);
    }

}
