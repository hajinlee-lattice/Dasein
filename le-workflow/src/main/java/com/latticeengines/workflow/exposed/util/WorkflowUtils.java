package com.latticeengines.workflow.exposed.util;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

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
    public static void updateJobFromYarn(Job job, WorkflowJob workflowJob, JobProxy jobProxy,
            WorkflowJobEntityMgr workflowJobEntityMgr) {
        if (job.getApplicationId() != null) {
            if (workflowJob.getStatus() == null || workflowJob.getStatus() == FinalApplicationStatus.UNDEFINED) {
                try {
                    com.latticeengines.domain.exposed.dataplatform.JobStatus status = jobProxy.getJobStatus(job
                            .getApplicationId());
                    workflowJob = workflowJobEntityMgr.updateStatusFromYarn(workflowJob, status);
                } catch (Exception e) {
                    // pass
                }
            }

            job.setJobStatus(getJobStatusFromFinalApplicationStatus(workflowJob.getStatus()));
        }
    }

    private static JobStatus getJobStatusFromFinalApplicationStatus(FinalApplicationStatus status) {
        if (YarnUtils.FAILED_STATUS.contains(status)) {
            return JobStatus.FAILED;
        } else if (status == FinalApplicationStatus.UNDEFINED || status == null) {
            return JobStatus.PENDING;
        } else {
            return JobStatus.COMPLETED;
        }
    }
}
