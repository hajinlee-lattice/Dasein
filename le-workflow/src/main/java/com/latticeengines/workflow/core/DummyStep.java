package com.latticeengines.workflow.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("dummyStep")
public class DummyStep extends AbstractStep<BaseStepConfiguration> {

    private static final Log log = LogFactory.getLog(DummyStep.class);

    @Override
    public void execute() {
        log.info("Inside DummyStep execute()");
    }

}
