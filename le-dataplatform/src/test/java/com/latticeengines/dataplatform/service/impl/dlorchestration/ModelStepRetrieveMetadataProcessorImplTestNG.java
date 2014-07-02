package com.latticeengines.dataplatform.service.impl.dlorchestration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@SuppressWarnings("unused")
public class ModelStepRetrieveMetadataProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ModelStepRetrieveMetadataProcessorImplTestNG.class);

    @Autowired
    private ModelStepRetrieveMetadataProcessorImpl modelStepRetrieveMetadataProcessor;

    protected boolean doYarnClusterSetup() {
        return false;
    }

    @Test(groups = "functional")
    public void testSuccessfulExecuteStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        // TODO need a more realistic test endpoint
        commandParameters.setDlUrl("http://httpbin.org/post");
        modelStepRetrieveMetadataProcessor.setLoginUrlSuffix("");
        modelStepRetrieveMetadataProcessor.setQueryMetadataUrlSuffix("");

        modelStepRetrieveMetadataProcessor.executeStep(command, commandParameters);
     }

    @Test(groups = "functional", expectedExceptions=LedpException.class)
    public void testFailedExecuteStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters();
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        // TODO need a more realistic test endpoint
        commandParameters.setDlUrl("http://httpbin.org/badurl");
        modelStepRetrieveMetadataProcessor.setLoginUrlSuffix("");
        modelStepRetrieveMetadataProcessor.setQueryMetadataUrlSuffix("");

        modelStepRetrieveMetadataProcessor.executeStep(command, commandParameters);
     }

 }
