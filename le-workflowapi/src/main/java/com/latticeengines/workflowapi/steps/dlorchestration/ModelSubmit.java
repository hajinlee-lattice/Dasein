package com.latticeengines.workflowapi.steps.dlorchestration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("modelSubmit")
public class ModelSubmit extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ModelSubmit.class);

    @Override
    public void execute() {
        log.info("Inside ModelSubmit execute()");
    }

}
