package com.latticeengines.workflowapi.steps.dlorchestration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("modelGenerateSamples")
public class ModelGenerateSamples extends AbstractStep<BaseStepConfiguration> {

    private static final Log log = LogFactory.getLog(ModelGenerateSamples.class);

    @Override
    public void execute() {
        log.info("Inside ModelGenerateSamples execute()");
    }

}
