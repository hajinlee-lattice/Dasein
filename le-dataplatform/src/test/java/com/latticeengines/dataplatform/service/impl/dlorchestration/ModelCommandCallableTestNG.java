package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepPostProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

@SuppressWarnings("unused")
public class ModelCommandCallableTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Autowired
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Autowired
    private ModelStepYarnProcessor modelStepYarnProcessor;

    @Autowired
    private ModelCommandLogService modelCommandLogService;

    @Autowired
    private ModelCommandLogEntityMgr modelCommandLogEntityMgr;

    @Autowired
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @Autowired
    private ModelStepPostProcessor modelStepFinishProcessor;

    @Autowired
    private ModelStepPostProcessor modelStepOutputResultsProcessor;

    protected boolean doDependencyLibraryCopy() {
        return false;
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "functional")
    public void testWorkflow() throws Exception {
        // Commented out tests until we get test rollback
/*
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        modelCommandEntityMgr.create(command);
        // List<ModelCommand> commands =
        // modelCommandEntityMgr.getNewAndInProgress();
        // ModelCommand command = commands.get(0);
        ModelCommandCallable modelCommandCallable = new ModelCommandCallable(command, jobService,
                modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepYarnProcessor, modelCommandLogService,
                modelCommandResultEntityMgr, modelStepFinishProcessor, modelStepOutputResultsProcessor);

        int iterations = 0;
        while ((command.getCommandStatus() == ModelCommandStatus.NEW || command.getCommandStatus() == ModelCommandStatus.IN_PROGRESS)
                && iterations < 400) {
            modelCommandCallable.call();
            iterations++;
            Thread.sleep(3000);
        }

        assertTrue(command.getCommandStatus() == ModelCommandStatus.SUCCESS);

        List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
        assertEquals(logs.size(), 12);
        List<ModelCommandState> states = modelCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 6);
*/
    }
    /*
     * @Test(groups = "functional") public void testModelCommandEntityMgr() {
     * ModelCommand command = createModelCommand(); List<ModelCommand> commands
     * = modelCommandEntityMgr.getNewAndInProgress();
     * assertEquals(commands.size(), 1); }
     */
}
