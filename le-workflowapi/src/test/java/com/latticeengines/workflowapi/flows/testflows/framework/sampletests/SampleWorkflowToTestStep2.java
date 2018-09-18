package com.latticeengines.workflowapi.flows.testflows.framework.sampletests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("sampleWorkflowToTestStep2")
public class SampleWorkflowToTestStep2 extends BaseWorkflowStep<SampleWorkflowToTestStep2Configuration> {

    private static final Logger log = LoggerFactory.getLogger(SampleWorkflowToTestStep2.class);

    @Override
    public void execute() {
        log.error("In SampleWorkflowToTestStep2.execute");

    }
}
