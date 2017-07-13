package com.latticeengines.workflow.functionalframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("sleepableStep")
public class SleepableStep extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SleepableStep.class);

    private long sleepTime = 0L;

    @Override
    public void execute() {
        log.info("Sleeping inside SleepableStep execute()");
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            // Do nothing
        }
        log.info("Done SleepableStep execute()");
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

}
