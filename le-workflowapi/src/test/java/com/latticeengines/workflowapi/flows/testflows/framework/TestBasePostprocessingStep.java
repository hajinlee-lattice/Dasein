package com.latticeengines.workflowapi.flows.testflows.framework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

// Base class for tearing down common test infrastructure.
// TODO: Fill this class in with needed postprocessing steps.
public class TestBasePostprocessingStep<C extends TestBasePostprocessingStepConfiguration> extends BaseWorkflowStep<C> {

    private static final Logger log = LoggerFactory.getLogger(TestBasePostprocessingStep.class);

    @Override
    public void execute() {
    }
}
