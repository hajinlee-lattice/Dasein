package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public class ModelStepYarnProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ModelStepYarnProcessorImplTestNG.class);

    private static final int NUM_ALGORITHMS = 1; // No LR; only RF for now.

    @Autowired
    private ModelStepYarnProcessorImpl modelStepYarnProcessor;

    protected boolean doDependencyLibraryCopy() {
        return false;
    }

    @Test(groups = "functional")
    public void testExecuteYarnSteps() throws Exception {
        cleanUpHdfs("Nutanix");
        cleanUpHdfs(CustomerSpace.parse("Nutanix").toString());
        setupDBConfig();
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        List<ApplicationId> appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.LOAD_DATA,
                command, commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.LOAD_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.GENERATE_SAMPLES, command,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.GENERATE_SAMPLES);

        appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.PROFILE_DATA, command,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.PROFILE_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.SUBMIT_MODELS, command,
                commandParameters);
        waitForSuccess(ModelingServiceTestUtils.NUM_SAMPLES * NUM_ALGORITHMS, appIds, ModelCommandStep.SUBMIT_MODELS);
    }

    @Test(groups = "functional")
    public void testExecuteYarnStepsFeatureThreshold() throws Exception {
        cleanUpHdfs("Nutanix");
        cleanUpHdfs(CustomerSpace.parse("Nutanix").toString());
        setupDBConfig();
        int featureThreshold = 30;
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L, featureThreshold);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        List<ApplicationId> appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.LOAD_DATA,
                command, commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.LOAD_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.GENERATE_SAMPLES, command,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.GENERATE_SAMPLES);

        appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.PROFILE_DATA, command,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.PROFILE_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.SUBMIT_MODELS, command,
                commandParameters);
        waitForSuccess(ModelingServiceTestUtils.NUM_SAMPLES * NUM_ALGORITHMS, appIds, ModelCommandStep.SUBMIT_MODELS);
    }

    private void setupDBConfig() {
        modelStepYarnProcessor.setDBConfig(dataSourceHost, dataSourcePort, dataSourceDB, dataSourceUser,
                dataSourcePasswd, dataSourceDBType);
    }

    private void waitForSuccess(int expectedNumAppIds, List<ApplicationId> appIds, ModelCommandStep step)
            throws Exception {
        log.info(step + ": Waiting for these appIds to succeed: " + Joiner.on(",").join(appIds));
        assertEquals(expectedNumAppIds, appIds.size());
        for (ApplicationId appId : appIds) {
            FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
            log.info(step + ": appId succeeded: " + appId.toString());
        }

    }
}
