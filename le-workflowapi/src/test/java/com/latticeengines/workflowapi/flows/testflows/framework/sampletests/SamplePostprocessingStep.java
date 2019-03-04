package com.latticeengines.workflowapi.flows.testflows.framework.sampletests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePostprocessingStep;

// Sample step showing how to extend the TestBasePostprocessingStep with test-specific tear down
// infrastructure.
@Component("samplePostprocessingStep")
public class SamplePostprocessingStep extends TestBasePostprocessingStep<SamplePostprocessingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SamplePostprocessingStep.class);

    @Override
    public void execute() {
        log.error("In SamplePostprocessingStep.execute");
        super.execute();
    }
}
