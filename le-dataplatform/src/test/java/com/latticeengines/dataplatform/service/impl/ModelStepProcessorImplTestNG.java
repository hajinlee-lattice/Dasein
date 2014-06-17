package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.ModelStepProcessor;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public class ModelStepProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {
    
    private static final Log log = LogFactory.getLog(ModelStepProcessorImplTestNG.class);
        
    private static final int NUM_ALGORITHMS = 1; // No LR; only RF for now.
            
    @Autowired
    private ModelStepProcessor modelStepProcessor;
    
    @Autowired
    private ModelingService modelingService;
    
    protected boolean doDependencyLibraryCopy() {
        return false;
    }    
    
    @Test(groups = "functional.scheduler")
    public void testExecuteYarnStep() throws Exception {        
        List<ModelCommandParameter> commandParameters = ModelingServiceTestUtils.createModelCommandWithCommandParameters().getCommandParameters();
        
        List<ApplicationId> appIds = modelStepProcessor.executeYarnStep("Nutanix", ModelCommandStep.LOAD_DATA, commandParameters);
        waitForSuccess(1*NUM_ALGORITHMS, appIds, 180, ModelCommandStep.LOAD_DATA);
        
        appIds = modelStepProcessor.executeYarnStep("Nutanix", ModelCommandStep.GENERATE_SAMPLES, commandParameters);
        waitForSuccess(1*NUM_ALGORITHMS, appIds, 180, ModelCommandStep.GENERATE_SAMPLES);

        appIds = modelStepProcessor.executeYarnStep("Nutanix", ModelCommandStep.PROFILE_DATA, commandParameters);
        waitForSuccess(1*NUM_ALGORITHMS, appIds, 180, ModelCommandStep.PROFILE_DATA);
        
        appIds = modelStepProcessor.executeYarnStep("Nutanix", ModelCommandStep.SUBMIT_MODELS, commandParameters);
        waitForSuccess(ModelingServiceTestUtils.NUM_SAMPLES*NUM_ALGORITHMS, appIds, 360, ModelCommandStep.SUBMIT_MODELS);
    }
    
    private void waitForSuccess(int expectedNumAppIds, List<ApplicationId> appIds, int secondsToWait, ModelCommandStep step) throws Exception {
        log.info(step + ": Waiting for these appIds to succeed: " + Joiner.on(",").join(appIds));
        assertEquals(expectedNumAppIds, appIds.size());
        for (ApplicationId appId : appIds) {
            FinalApplicationStatus status = waitForStatus(appId, secondsToWait, TimeUnit.SECONDS,
                    FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
            log.info(step + ": appId succeeded: " + appId.toString());
        }
        
    }
}
