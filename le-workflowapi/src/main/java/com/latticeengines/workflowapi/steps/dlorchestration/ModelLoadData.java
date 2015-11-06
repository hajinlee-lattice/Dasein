package com.latticeengines.workflowapi.steps.dlorchestration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("modelLoadData")
public class ModelLoadData extends AbstractStep<ModelLoadDataConfiguration> {

    private static final Log log = LogFactory.getLog(ModelLoadData.class);

    @Override
    public void execute() {
        log.info("Inside ModelLoadData execute()");
        log.info("Configuration value: " + configuration.getI());
    }

}
