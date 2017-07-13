package com.latticeengines.workflow.functionalframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("anotherSuccessfulStep")
public class AnotherSuccessfulStep extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AnotherSuccessfulStep.class);

    @Override
    public void execute() {
        log.info("Inside AnotherSuccessfulStep execute()");
    }

}
