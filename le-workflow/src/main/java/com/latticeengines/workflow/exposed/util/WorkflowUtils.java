package com.latticeengines.workflow.exposed.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;

public class WorkflowUtils {
    /**
     * Side-effect of this method is to store the latest status into the
     * WorkflowJob.
     */

    private static final Log log = LogFactory.getLog(WorkflowUtils.class);

    public static void updateJobFromYarn(Job job, WorkflowJob workflowJob, JobProxy jobProxy,
            WorkflowJobEntityMgr workflowJobEntityMgr) {
        YarnApplicationState jobState = null;

        if (workflowJob.getApplicationId() != null
                && (workflowJob.getStatus() == null || workflowJob.getStatus() == FinalApplicationStatus.UNDEFINED)) {
            try {
                com.latticeengines.domain.exposed.dataplatform.JobStatus status = jobProxy
                        .getJobStatus(workflowJob.getApplicationId());
                jobState = status.getState();
                workflowJob = workflowJobEntityMgr.updateStatusFromYarn(workflowJob, status);
            } catch (Exception e) {
                log.warn("Not able to find job status from yarn with applicationId:" + job.getApplicationId()
                        + ".  Assuming it failed and was purged from the system.");
                try {
                    com.latticeengines.domain.exposed.dataplatform.JobStatus terminal = getTerminalStatus(workflowJob);
                    jobState = terminal.getState();
                    workflowJob = workflowJobEntityMgr.updateStatusFromYarn(workflowJob, terminal);
                } catch (Exception inner) {
                    log.error("Failed to update WorkflowJob so that successive queries aren't necessary", inner);
                }
            }
        }
        // We only trust the WorkflowJob status if it is non-null
        JobStatus status = getJobStatusFromFinalApplicationStatus(workflowJob.getStatus(), jobState);
        if (status != null) {
            job.setJobStatus(status);
        }
    }

    private static com.latticeengines.domain.exposed.dataplatform.JobStatus getTerminalStatus(WorkflowJob workflowJob) {
        com.latticeengines.domain.exposed.dataplatform.JobStatus terminal = new com.latticeengines.domain.exposed.dataplatform.JobStatus();
        terminal.setStatus(FinalApplicationStatus.FAILED);
        terminal.setState(YarnApplicationState.KILLED);
        if (workflowJob.getStartTimeInMillis() != null) {
            terminal.setStartTime(workflowJob.getStartTimeInMillis());
        }
        return terminal;
    }

    private static JobStatus getJobStatusFromFinalApplicationStatus(FinalApplicationStatus status,
            YarnApplicationState jobState) {
        if (jobState == YarnApplicationState.RUNNING) {
            return JobStatus.RUNNING;
        }

        if (YarnUtils.FAILED_STATUS.contains(status)) {
            return JobStatus.FAILED;
        } else if (status == FinalApplicationStatus.UNDEFINED) {
            return JobStatus.PENDING;
        } else {
            return null;
        }
    }
}
