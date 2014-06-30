package com.latticeengines.dataplatform.service.impl.dlorchestration;

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
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;

@SuppressWarnings("unused")
public class ModelStepOutputResultsProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {
    
    private static final Log log = LogFactory.getLog(ModelStepOutputResultsProcessorImplTestNG.class);

    @Autowired
    private ModelStepProcessor modelStepOutputResultsProcessor;

    private ModelCommandCallable modelCommandCallable = new ModelCommandCallable(null, null, null, null, null, null,
            null, null, null);

    protected boolean doYarnClusterSetup() {
        return false;
    }

    @Test(groups = "functional")
    public void testExecutePostStep() throws Exception {
        List<ModelCommandParameter> listParameters = ModelingServiceTestUtils.createModelCommandWithCommandParameters()
                .getCommandParameters();
        ModelCommandParameters commandParameters = modelCommandCallable.validateAndSetCommandParameters(listParameters);
        commandParameters.setEventTable("ATable");
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
//        modelCommandEntityMgr.create(command);

//        modelStepOutputResultsProcessor.executePostStep(command, commandParameters);
     }

 }
