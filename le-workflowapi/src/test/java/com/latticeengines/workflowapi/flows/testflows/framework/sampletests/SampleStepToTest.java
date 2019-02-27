package com.latticeengines.workflowapi.flows.testflows.framework.sampletests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

// Example of single step that could be tested with the framework.
@Component("sampleStepToTest")
public class SampleStepToTest extends BaseWorkflowStep<SampleStepToTestConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SampleStepToTest.class);

    @Override
    public void execute() {
        log.error("In SampleStepToTest.execute");

    }
}
