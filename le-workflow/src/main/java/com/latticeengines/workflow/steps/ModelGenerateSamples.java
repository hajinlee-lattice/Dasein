package com.latticeengines.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.WorkflowStep;

@Component("modelGenerateSamples")
public class ModelGenerateSamples extends WorkflowStep {

    private static final Log log = LogFactory.getLog(ModelGenerateSamples.class);

    @Override
    public void execute() {
        log.info("Inside ModelGenerateSamples execute()");
    }

}
