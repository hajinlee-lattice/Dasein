package com.latticeengines.workflowapi.steps.dlorchestration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("modelLoadData")
public class ModelLoadData extends AbstractStep<ModelLoadDataConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ModelLoadData.class);

    @Override
    public void execute() {
        log.info("Inside ModelLoadData execute()");
        log.info("Configuration value: " + configuration.getI());
    }

}
