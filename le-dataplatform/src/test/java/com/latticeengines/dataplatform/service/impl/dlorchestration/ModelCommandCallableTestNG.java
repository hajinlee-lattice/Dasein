package com.latticeengines.dataplatform.service.impl.dlorchestration;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.ModelCommandLogService;
import com.latticeengines.dataplatform.service.ModelStepProcessor;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

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
        
    private ModelCommandCallable modelCommandCallable;
 
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        ModelCommand modelCommand = new ModelCommand();
        modelCommandCallable = new ModelCommandCallable(modelCommand, jobService, modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepProcessor, modelCommandLogService);
    }
    
    @Test(groups = "functional")
    public void testWorkflow() throws Exception {
     // TODO uncomment this when persistence is comlete - modelCommandCallable.call();
        // call this in a loop (to simulate) multiple Scheduler invocations until modelCommand reaches SUCCESS state.
        
        // validate log table output
        
        // validate full set of step-appIds in State table
    }
    
    // TODO Test Failure cases
}
