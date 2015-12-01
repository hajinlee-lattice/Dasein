package com.latticeengines.workflowapi.steps.dlorchestration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("modelSubmit")
public class ModelSubmit extends AbstractStep<BaseStepConfiguration> {

    private static final Log log = LogFactory.getLog(ModelSubmit.class);

    @Override
    public void execute() {
        log.info("Inside ModelSubmit execute()");
    }

}
