package com.latticeengines.workflow.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractStep;

@Component("sleepableStep")
public class SleepableStep extends AbstractStep {

    private static final Log log = LogFactory.getLog(SleepableStep.class);

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
