package com.latticeengines.release.processes;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.release.exposed.activities.Activity;
import com.latticeengines.release.exposed.domain.ProcessContext;
import com.latticeengines.release.exposed.domain.StatusContext;

public class ReleaseProcess {

    private Log log = LogFactory.getLog(ReleaseProcess.class);

    private List<Activity> activities;

    @Autowired
    private ProcessContext processContext;

    public ReleaseProcess(List<Activity> activities) {
        this.activities = activities;
    }

    public List<Activity> getActivities() {
        return this.activities;
    }

    public void execute() {
        System.out.println(processContext);
        if (CollectionUtils.isEmpty(activities)) {
            throw new RuntimeException("Process should contain some activities");
        }
        for (Activity activity : activities) {
            StatusContext statusContext = activity.execute();
            log.info(activity.getBeanName() + " Status code: " + statusContext.getStatusCode());
            log.info(activity.getBeanName() + " Response message: " + statusContext.getResponseMessage());
            if (log.isDebugEnabled())
                log.debug("running activity:" + activity + " using arguments:" + processContext);
            if (processShouldStop(statusContext, activity))
                throw new RuntimeException("The release process failed!");
        }
    }

    private boolean processShouldStop(StatusContext statusContext, Activity activity) {
        if (statusContext != null && statusContext.stopProcess()) {
            log.error("Interrupted workflow as requested by:" + activity.getBeanName());
            return true;
        }
        return false;
    }
}
