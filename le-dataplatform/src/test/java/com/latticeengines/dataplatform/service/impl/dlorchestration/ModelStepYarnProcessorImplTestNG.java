package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

@Deprecated
public class ModelStepYarnProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ModelStepYarnProcessorImplTestNG.class);

    private static final int NUM_ALGORITHMS = 1; // No LR; only RF for now.

    private static final String CUSTOMER = ModelStepYarnProcessorImplTestNG.class.getSimpleName();

    @Autowired
    private ModelStepYarnProcessorImpl modelStepYarnProcessor;

    protected boolean doDependencyLibraryCopy() {
        return false;
    }

    @Test(groups = "sqoop", enabled = false)
    public void testExecuteYarnSteps() throws Exception {
        cleanUpHdfs(CUSTOMER);
        cleanUpHdfs(CustomerSpace.parse(CUSTOMER).toString());
        setupDBConfig();
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        List<ApplicationId> appIds = modelStepYarnProcessor.executeYarnStep(CUSTOMER, ModelCommandStep.LOAD_DATA,
                command, commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.LOAD_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep(CUSTOMER, ModelCommandStep.GENERATE_SAMPLES, command,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.GENERATE_SAMPLES);

        appIds = modelStepYarnProcessor.executeYarnStep(CUSTOMER, ModelCommandStep.PROFILE_DATA, command,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.PROFILE_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep(CUSTOMER, ModelCommandStep.SUBMIT_MODELS, command,
                commandParameters);
        waitForSuccess(ModelingServiceTestUtils.NUM_SAMPLES * NUM_ALGORITHMS, appIds, ModelCommandStep.SUBMIT_MODELS);
    }

    @Test(groups = "sqoop", enabled = false)
    public void testExecuteYarnStepsFeatureThreshold() throws Exception {
        cleanUpHdfs(CUSTOMER);
        cleanUpHdfs(CustomerSpace.parse(CUSTOMER).toString());
        setupDBConfig();
        int featureThreshold = 30;
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L, featureThreshold);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        List<ApplicationId> appIds = modelStepYarnProcessor.executeYarnStep(CUSTOMER, ModelCommandStep.LOAD_DATA,
                command, commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.LOAD_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep(CUSTOMER, ModelCommandStep.GENERATE_SAMPLES, command,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.GENERATE_SAMPLES);

        appIds = modelStepYarnProcessor.executeYarnStep(CUSTOMER, ModelCommandStep.PROFILE_DATA, command,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.PROFILE_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep(CUSTOMER, ModelCommandStep.SUBMIT_MODELS, command,
                commandParameters);
        waitForSuccess(ModelingServiceTestUtils.NUM_SAMPLES * NUM_ALGORITHMS, appIds, ModelCommandStep.SUBMIT_MODELS);
    }

    @Test(groups = "sqoop", enabled = false)
    public void testModelSelectionUsingCamille() throws Exception {
        int featureThreshold = -1;
        String modelingServiceName = "Modeling";
        String customer = "testCustomer";

        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customer), modelingServiceName);
        try {
            camille.delete(docPath);
        } catch (Exception e) {
        }

        Path featuresThresholdDocPath = docPath.append("FeaturesThreshold");
        try {
            camille.create(featuresThresholdDocPath, new Document(String.valueOf(featureThreshold)),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            log.error("Failed to create FeaturesThreshold document in Camille. " + e.toString());
        }
    }

    @Deprecated
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
