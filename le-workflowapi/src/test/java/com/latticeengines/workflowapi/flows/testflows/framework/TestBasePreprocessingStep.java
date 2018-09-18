package com.latticeengines.workflowapi.flows.testflows.framework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

// Base class for setting up common test infrastructure.
// TODO: Fill this class in with needed preprocessing steps.
public class TestBasePreprocessingStep<C extends TestBasePreprocessingStepConfiguration> extends BaseWorkflowStep<C> {

    private static final Logger log = LoggerFactory.getLogger(TestBasePreprocessingStep.class);

    @Override
    public void execute() {
    }
}
