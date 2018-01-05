package com.latticeengines.workflow.exposed.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.exception.RemoteLedpException;
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

    private static final Logger log = LoggerFactory.getLogger(WorkflowUtils.class);

    public static void updateJobFromYarn(Job job, WorkflowJob workflowJob, JobProxy jobProxy,
            WorkflowJobEntityMgr workflowJobEntityMgr) {
        YarnApplicationState jobState = null;

        if (workflowJob.getApplicationId() != null &&
                (StringUtils.isEmpty(workflowJob.getStatus()) ||
                 workflowJob.getStatus().equals(FinalApplicationStatus.UNDEFINED.name()) ||
                 !JobStatus.valueOf(workflowJob.getStatus()).isTerminated())) {
            try {
                com.latticeengines.domain.exposed.dataplatform.JobStatus status = jobProxy.getJobStatus(
                        workflowJob.getApplicationId());
                if (FinalApplicationStatus.SUCCEEDED.equals(status.getStatus()) ||
                        FinalApplicationStatus.FAILED.equals(status.getStatus()) ||
                        FinalApplicationStatus.KILLED.equals(status.getStatus())) {
                    jobState = status.getState();
                    workflowJob = workflowJobEntityMgr.updateStatusFromYarn(workflowJob, status);
                }
            } catch (RemoteLedpException exc) {
                log.warn(String.format("Not able to find job status from yarn with applicationId: %s. Assuming it " +
                        "failed and was purged from the system.", job.getApplicationId()), exc);
                try {
                    com.latticeengines.domain.exposed.dataplatform.JobStatus terminal = getTerminalStatus(workflowJob);
                    jobState = terminal.getState();
                    workflowJob = workflowJobEntityMgr.updateStatusFromYarn(workflowJob, terminal);
                } catch (Exception inner) {
                    log.error("Failed to update WorkflowJob so that successive queries aren't necessary", inner);
                }
            } catch (Exception exc) {
                log.warn("some database related exception ocurred", exc);
            }
        }
        // We only trust the WorkflowJob status if it is non-null
        String status = workflowJob.getStatus();
        JobStatus jobStatus = JobStatus.fromString(status, jobState);
        if (jobStatus != null) {
            job.setJobStatus(jobStatus);
        }
    }

    private static com.latticeengines.domain.exposed.dataplatform.JobStatus getTerminalStatus(WorkflowJob workflowJob) {
        com.latticeengines.domain.exposed.dataplatform.JobStatus terminal =
                new com.latticeengines.domain.exposed.dataplatform.JobStatus();
        terminal.setStatus(FinalApplicationStatus.FAILED);
        terminal.setState(YarnApplicationState.KILLED);
        if (workflowJob.getStartTimeInMillis() != null) {
            terminal.setStartTime(workflowJob.getStartTimeInMillis());
        }
        return terminal;
    }
}
