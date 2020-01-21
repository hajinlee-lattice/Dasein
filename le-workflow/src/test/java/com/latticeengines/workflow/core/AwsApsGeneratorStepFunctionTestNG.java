package com.latticeengines.workflow.core;

import java.util.Arrays;

import javax.inject.Inject;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import com.latticeengines.aws.batch.BatchService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;
public class AwsApsGeneratorStepFunctionTestNG extends WorkflowTestNGBase {

    @Inject
    private BatchService batchService;

    @Test(groups = "manual", enabled = true)
    public void getJobStatusForJobCantFindInYarnWithSuccessfulStatus() {
        BaseAwsPythonBatchStep<AWSPythonBatchConfiguration> step = new BaseAwsPythonBatchStep<AWSPythonBatchConfiguration>() {
            @Override
            protected String getCondaEnv() {
                return "v01";
            }

            @Override
            protected String getPythonScript() {
                return "apsgenerator.py";
            }

            @Override
            protected void localizePythonScripts() {
            }
        };

        AWSPythonBatchConfiguration config = new AWSPythonBatchConfiguration();
        config.setRunInAws(true);
        config.setInputPaths(Arrays.asList("/Pods/Aps/input/*.avro"));
        config.setOutputPath("/Pods/Aps/output");
        config.setCustomerSpace(CustomerSpace.parse("ApsTest"));
        step.webHdfs = "http://webhdfs.lattice.local:14000/webhdfs/v1";
        ReflectionTestUtils.setField(step, "batchService", batchService);
        step.setConfiguration(config);
        step.execute();

    }
}
