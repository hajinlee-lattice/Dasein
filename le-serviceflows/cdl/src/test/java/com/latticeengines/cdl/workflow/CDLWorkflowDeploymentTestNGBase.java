package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.testng.log4testng.Logger;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsWorkflowDeploymentTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml", "classpath:test-serviceflows-cdl-context.xml" })
public abstract class CDLWorkflowDeploymentTestNGBase extends ServiceFlowsWorkflowDeploymentTestNGBase {

    private static Logger log = Logger.getLogger(CDLWorkflowDeploymentTestNGBase.class);

    @Inject
    private WorkflowProxy workflowProxy;

    protected JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        int retryOnException = 4;
        Job job;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId,
                        CustomerSpace.parse(currentTestTenant().getId()).toString());
            } catch (Exception e) {
                log.error(String.format("Workflow job exception: %s", e.getMessage()), e);

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                if (job.getJobStatus() == JobStatus.FAILED) {
                    log.error(applicationId + " Failed with ErrorCode " + job.getErrorCode() + ". \n"
                            + job.getErrorMsg());
                }
                return job.getJobStatus();
            }
            try {
                log.info("Waiting for workflow to finish: " + applicationId);
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public abstract Tenant currentTestTenant();

}
