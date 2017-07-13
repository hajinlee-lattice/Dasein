package com.latticeengines.workflowapi.steps.dlorchestration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("modelGenerateSamples")
public class ModelGenerateSamples extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ModelGenerateSamples.class);

    @Override
    public void execute() {
        log.info("Inside ModelGenerateSamples execute()");
    }

}
