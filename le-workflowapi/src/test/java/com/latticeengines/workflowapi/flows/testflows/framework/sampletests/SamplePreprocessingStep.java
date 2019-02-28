package com.latticeengines.workflowapi.flows.testflows.framework.sampletests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePreprocessingStep;

// Sample step showing how to extend the TestBasePreprocessingStep with test-specific setup
// infrastructure.
@Component("samplePreprocessingStep")
public class SamplePreprocessingStep extends TestBasePreprocessingStep<SamplePreprocessingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SamplePreprocessingStep.class);

    @Override
    public void execute() {
        log.error("In SamplePreprocessingStep.execute");
        super.execute();
    }
}
