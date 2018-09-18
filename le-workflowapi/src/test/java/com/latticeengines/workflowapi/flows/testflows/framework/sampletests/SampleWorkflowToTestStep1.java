package com.latticeengines.workflowapi.flows.testflows.framework.sampletests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("sampleWorkflowToTestStep1")
public class SampleWorkflowToTestStep1 extends BaseWorkflowStep<SampleWorkflowToTestStep1Configuration> {

    private static final Logger log = LoggerFactory.getLogger(SampleWorkflowToTestStep1.class);

    @Override
    public void execute() {
        log.error("In SampleWorkflowToTestStep1.execute");

    }
}
