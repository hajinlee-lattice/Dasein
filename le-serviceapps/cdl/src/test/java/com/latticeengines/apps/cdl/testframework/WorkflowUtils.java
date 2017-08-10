package com.latticeengines.apps.cdl.testframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class WorkflowUtils {

    private static final Logger log = LoggerFactory.getLogger(WorkflowUtils.class);

    private final WorkflowProxy workflowProxy;

    public WorkflowUtils(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
    }

    public JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        int retryOnException = 4;
        Job job = null;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
            } catch (Exception e) {
                log.error(String.format("Workflow job exception: %s", e.getMessage()), e);

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                return job.getJobStatus();
            }
            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
