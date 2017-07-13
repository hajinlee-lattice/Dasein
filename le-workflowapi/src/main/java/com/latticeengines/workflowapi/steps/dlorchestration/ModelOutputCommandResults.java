package com.latticeengines.workflowapi.steps.dlorchestration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("modelOutputCommandResults")
public class ModelOutputCommandResults extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ModelOutputCommandResults.class);

    @Override
    public void execute() {
        log.info("Inside ModelOutputCommandResults execute()");
    }

}
