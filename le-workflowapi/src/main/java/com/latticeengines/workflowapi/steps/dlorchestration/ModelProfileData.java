package com.latticeengines.workflowapi.steps.dlorchestration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseStepConfiguration;

@Component("modelProfileData")
public class ModelProfileData extends AbstractStep<BaseStepConfiguration> {

    private static final Log log = LogFactory.getLog(ModelProfileData.class);

    @Override
    public void execute() {
        log.info("Inside ModelProfileData execute()");
    }

}
