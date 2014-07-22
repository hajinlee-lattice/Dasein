package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public class ModelStepYarnProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ModelStepYarnProcessorImplTestNG.class);

    private static final int NUM_ALGORITHMS = 1; // No LR; only RF for now.

    @Autowired
    private ModelStepYarnProcessor modelStepYarnProcessor;

    @Autowired
    private ModelingService modelingService;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path("/user/s-analytics/customers/Nutanix"), true);
    }
    
    protected boolean doDependencyLibraryCopy() {
        return false;
    }

    @Test(groups = "functional.scheduler")
    public void testExecuteYarnSteps() throws Exception {
        List<ModelCommandParameter> listParameters = ModelingServiceTestUtils.createModelCommandWithCommandParameters()
                .getCommandParameters();
        ModelCommandParameters commandParameters = new ModelCommandParameters(listParameters);

        List<ApplicationId> appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.LOAD_DATA,
                commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.LOAD_DATA);

        appIds = modelStepYarnProcessor
                .executeYarnStep("Nutanix", ModelCommandStep.GENERATE_SAMPLES, commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.GENERATE_SAMPLES);

        appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.PROFILE_DATA, commandParameters);
        waitForSuccess(1 * NUM_ALGORITHMS, appIds, ModelCommandStep.PROFILE_DATA);

        appIds = modelStepYarnProcessor.executeYarnStep("Nutanix", ModelCommandStep.SUBMIT_MODELS, commandParameters);
        waitForSuccess(ModelingServiceTestUtils.NUM_SAMPLES * NUM_ALGORITHMS, appIds, ModelCommandStep.SUBMIT_MODELS);
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
