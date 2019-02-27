package com.latticeengines.workflowapi.flows.testflows.framework.sampletests;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

// Example of single workflow that could be tested with the framework.
@Component("sampleWorkflowToTest")
public class SampleWorkflowToTest extends AbstractWorkflow<SampleWorkflowToTestConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SampleWorkflowToTest.class);

    @Inject
    private SampleWorkflowToTestStep1 sampleWorkflowToTestStep1;

    @Inject
    private SampleWorkflowToTestStep2 sampleWorkflowToTestStep2;

    @Override
    public Workflow defineWorkflow(SampleWorkflowToTestConfiguration config) {
        log.error("In SampleWorkflowToTest.defineWorkflow");

        return new WorkflowBuilder(name(), config) //
                .next(sampleWorkflowToTestStep1) //
                .next(sampleWorkflowToTestStep2) //
                .build();
    }
}
