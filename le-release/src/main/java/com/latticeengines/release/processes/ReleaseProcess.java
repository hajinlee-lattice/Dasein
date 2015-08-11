package com.latticeengines.release.processes;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.release.exposed.activities.Activity;
import com.latticeengines.release.exposed.domain.ProcessContext;

public class ReleaseProcess {

    private Log log = LogFactory.getLog(ReleaseProcess.class);

    private List<Activity> activities;

    public ReleaseProcess(List<Activity> activities) {
        this.activities = activities;
    }

    public List<Activity> getActivities() {
        return this.activities;
    }

    public void execute(ProcessContext context) {
        if (CollectionUtils.isEmpty(activities)) {
            throw new RuntimeException("Process should contain some activities");
        }
        for (Activity activity : activities) {
            activity.execute(context);
            log.info(activity.getBeanName() + " Status code: " + context.getStatusCode());
            log.info(activity.getBeanName() + " Response message: " + context.getResponseMessage());
            if (log.isDebugEnabled())
                log.debug("running activity:" + activity + " using arguments:" + context);
            if (processShouldStop(context, activity))
                break;
            context.setResponseMessage("");
            context.setStatusCode(-1);
        }
    }

    private boolean processShouldStop(ProcessContext context, Activity activity) {
        if (context != null && context.stopProcess()) {
            log.error("Interrupted workflow as requested by:" + activity.getBeanName());
            return true;
        }
        return false;
    }
}
