package com.latticeengines.workflow.exposed.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

public class WorkflowUtils {
    /**
     * Side-effect of this method is to store the latest status into the
     * WorkflowJob.
     */

    private static final Log log = LogFactory.getLog(WorkflowUtils.class);

    public static void updateJobFromYarn(Job job, WorkflowJob workflowJob, JobProxy jobProxy,
                                         WorkflowJobEntityMgr workflowJobEntityMgr, boolean fromWorkflowCache) {
        if (!fromWorkflowCache) {
            YarnApplicationState jobState = null;

            if (workflowJob.getStatus() == null || workflowJob.getStatus() == FinalApplicationStatus.UNDEFINED) {
                try {
                    com.latticeengines.domain.exposed.dataplatform.JobStatus status = jobProxy.getJobStatus(job
                            .getApplicationId());
                    jobState = status.getState();
                    workflowJob = workflowJobEntityMgr.updateStatusFromYarn(workflowJob, status);
                } catch (Exception e) {
                    log.warn("Not able to find job status from yarn with applicationId:" + job.getApplicationId());
                }
            }
            // We only trust the WorkflowJob status if it is non-null
            JobStatus status = getJobStatusFromFinalApplicationStatus(workflowJob.getStatus(), jobState);
            if (status != null) {
                job.setJobStatus(status);
            }
        }
    }

    private static JobStatus getJobStatusFromFinalApplicationStatus(FinalApplicationStatus status, YarnApplicationState jobState) {
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
