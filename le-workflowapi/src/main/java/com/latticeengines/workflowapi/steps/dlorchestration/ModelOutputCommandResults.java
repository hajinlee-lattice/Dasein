package com.latticeengines.workflowapi.steps.dlorchestration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("modelOutputCommandResults")
public class ModelOutputCommandResults extends AbstractStep<BaseStepConfiguration> {

    private static final Log log = LogFactory.getLog(ModelOutputCommandResults.class);

    @Override
    public void execute() {
        log.info("Inside ModelOutputCommandResults execute()");
    }

}
