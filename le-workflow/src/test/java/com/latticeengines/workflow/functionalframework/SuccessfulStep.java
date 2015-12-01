package com.latticeengines.workflow.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("successfulStep")
public class SuccessfulStep extends AbstractStep<BaseStepConfiguration> {

    private static final Log log = LogFactory.getLog(SuccessfulStep.class);

    @Override
    public void execute() {
        log.info("Inside SuccessfulStep execute()");
    }

}
