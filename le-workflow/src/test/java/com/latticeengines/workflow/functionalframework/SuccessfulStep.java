package com.latticeengines.workflow.functionalframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("successfulStep")
public class SuccessfulStep extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SuccessfulStep.class);

    @Override
    public void initialize() {
        this.setRunAgainWhenComplete(false);
    }

    @Override
    public void execute() {
        log.info("Inside SuccessfulStep execute()");
    }

}
