package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.ModelStepProcessor;
import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public class ModelStepProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {
    
    private static final Log log = LogFactory.getLog(ModelStepProcessorImplTestNG.class);
    
    private static final int MODEL_COMMAND_ID = 1;
            
    @Autowired
    private ModelStepProcessor modelStepProcessor;

      // TODO use this to test against SQL Server data
//    public List<ModelCommandParameter> createListModelCommandParameters() {
//        List<ModelCommandParameter> parameters = new ArrayList<>();
//        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.DEPIVOTED_EVENT_TABLE, "Q_EventTableDepivot_Nutanix"));
//        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.EVENT_TABLE, "Q_EventTable_Nutanix"));
//        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.KEY_COLS, "Nutanix_EventTable_Clean"));
//        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.METADATA_TABLE, "EventMetadata_Nutanix"));
//        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.MODEL_NAME, "Model Submission1"));
//        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.MODEL_TARGETS, "P1_Event_1"));
//        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.NUM_SAMPLES, "3"));
//        
//        return parameters;
//    }
   
    public List<ModelCommandParameter> createListModelCommandParameters() {
        List<ModelCommandParameter> parameters = new ArrayList<>();
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.DEPIVOTED_EVENT_TABLE, "iris"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.EVENT_TABLE, "iris2"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.KEY_COLS, "ID"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.METADATA_TABLE, "iris_metadata"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.MODEL_NAME, "Model Submission1"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.MODEL_TARGETS, "CATEGORY"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.NUM_SAMPLES, "3"));
        
        return parameters;
    }   
    
    @Test(groups = "functional")
    public void testExecuteJSONStep() {
        // TODO testExecuteJSONStep
    }
    
    @Test(groups = "functional")
    public void testExecuteYarnStep() throws Exception {
        List<ModelCommandParameter> commandParameters = createListModelCommandParameters();
        List<ApplicationId> appIds = modelStepProcessor.executeYarnStep("Nutanix", ModelCommandStep.LOAD_DATA, commandParameters);
        log.info("Waiting for these appIds to succeed: " + Joiner.on(",").join(appIds));
        
        for (ApplicationId appId : appIds) {
            FinalApplicationStatus status = waitForStatus(appId, 120, TimeUnit.SECONDS,
                    FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
            log.info("appId succeeded: " + appId.toString());
        }
        
        appIds = modelStepProcessor.executeYarnStep("Nutanix", ModelCommandStep.GENERATE_SAMPLES, commandParameters);
        log.info("Waiting for these appIds to succeed: " + Joiner.on(",").join(appIds));
        for (ApplicationId appId : appIds) {
            FinalApplicationStatus status = waitForStatus(appId, 120, TimeUnit.SECONDS,
                    FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
            log.info("appId succeeded: " + appId.toString());
        }
        
        appIds = modelStepProcessor.executeYarnStep("Nutanix", ModelCommandStep.SUBMIT_MODELS, commandParameters);
        log.info("Waiting for these appIds to succeed: " + Joiner.on(",").join(appIds));
        for (ApplicationId appId : appIds) {
            FinalApplicationStatus status = waitForStatus(appId, 240, TimeUnit.SECONDS,
                    FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
            log.info("appId succeeded: " + appId.toString());
        }
    }
}
