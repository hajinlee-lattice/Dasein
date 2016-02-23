package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

public class ModelCommandEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "functional")
    public void testGetNewAndInProgress() throws Exception {
        List<ModelCommand> commands = modelCommandEntityMgr.getNewAndInProgress();
        assertEquals(commands.size(), 0);

        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L);
        modelCommandEntityMgr.create(command);

        commands = modelCommandEntityMgr.getNewAndInProgress();
        assertEquals(commands.size(), 1);

        ModelCommand nonTahoeCommand = new ModelCommand(2L, "Nutanix", "Nutanix", ModelCommandStatus.NEW,
                new ArrayList<ModelCommandParameter>(), "NotTahoe", ModelingServiceTestUtils.EVENT_TABLE);
        modelCommandEntityMgr.create(nonTahoeCommand);
        commands = modelCommandEntityMgr.getNewAndInProgress();
        assertEquals(commands.size(), 1);
    }

    
}
