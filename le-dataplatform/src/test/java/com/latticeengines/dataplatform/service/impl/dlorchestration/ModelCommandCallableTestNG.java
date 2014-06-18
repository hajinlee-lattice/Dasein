package com.latticeengines.dataplatform.service.impl.dlorchestration;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.ModelCommandLogService;
import com.latticeengines.dataplatform.service.ModelStepProcessor;


public class ModelCommandCallableTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Autowired
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Autowired
    private ModelStepProcessor modelStepProcessor;

    @Autowired
    private ModelCommandLogService modelCommandLogService;
    
    @Autowired
    private ModelCommandLogEntityMgr modelCommandLogEntityMgr;

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
//        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
//        modelCommandEntityMgr.create(command);
        List<ModelCommand> commands = modelCommandEntityMgr.getNewAndInProgress();
        ModelCommand command = commands.get(0);
        ModelCommandCallable modelCommandCallable = new ModelCommandCallable(command, jobService,
                modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepProcessor, modelCommandLogService);

        int iterations = 0;
        while ((command.getCommandStatus() == ModelCommandStatus.NEW || command.getCommandStatus() == ModelCommandStatus.IN_PROGRESS)
                && iterations < 400) {            
            modelCommandCallable.call();
            iterations++;
            Thread.sleep(3000);
        }

        assertTrue(command.getCommandStatus() == ModelCommandStatus.SUCCESS);
               
        List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
        assertEquals(logs.size(), 10);
        List<ModelCommandState> states = modelCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 5); 
       */     
    }
/*    
    @Test(groups = "functional")
    public void testModelCommandEntityMgr() {
        ModelCommand command = createModelCommand();
        List<ModelCommand> commands = modelCommandEntityMgr.getNewAndInProgress();
        assertEquals(commands.size(), 1);
    }
*/
}
