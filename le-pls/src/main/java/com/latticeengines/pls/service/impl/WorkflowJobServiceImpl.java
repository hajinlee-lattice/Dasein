package com.latticeengines.pls.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("workflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {

    private static final Log log = LogFactory.getLog(WorkflowJobService.class);

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Override
    public AppSubmission restart(String jobId) {
        return workflowProxy.restartWorkflowExecution(jobId);
    }

    @Override
    public void cancel(String jobId) {
        workflowProxy.stopWorkflow(jobId);
    }

    @Override
    public List<Job> findAllWithType(String type) {
        Tenant tenantWithPid = getTenant();
        log.debug("Finding jobs for " + tenantWithPid.toString() + " with pid "
                + tenantWithPid.getPid());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid(), type);
        if (jobs == null) {
            jobs = Collections.emptyList();
        }
        return jobs;
    }

    @Override
    public Job findByApplicationId(String applicationId) {
        Tenant tenantWithPid = getTenant();
        log.debug("Finding job for application Id " + applicationId + " with pid "
                + tenantWithPid.getPid());
        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
        return job;
    }

    @Override
    public Job find(String jobId) {
        return workflowProxy.getWorkflowExecution(jobId);
    }

    @Override
    public List<Job> findAll() {
        Tenant tenantWithPid = getTenant();
        log.debug("Finding jobs for " + tenantWithPid.toString() + " with pid "
                + tenantWithPid.getPid());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        if (jobs == null) {
            jobs = Collections.emptyList();
        }
        updateJobsWithModelSummeries(jobs);
        return jobs;
    }

    private void updateJobsWithModelSummeries(List<Job> jobs) {
        Map<String, ModelSummary> modelIdToModelSummaries = new HashMap<>();
        for (ModelSummary modelSummary : modelSummaryService.getModelSummaries("all")) {
            modelIdToModelSummaries.put(modelSummary.getId(), modelSummary);
        }

        for (Job job : jobs) {
            if (job.getInputs() == null) {
                job.setInputs(new HashMap<String, String>());
            }
            job.getInputs().put(WorkflowContextConstants.Inputs.SOURCE_FILE_EXISTS,
                    getJobSourceFileExists(job.getApplicationId()).toString());

            String modelId = null;
            if (job.getInputs() != null
                    && job.getInputs().containsKey(WorkflowContextConstants.Inputs.MODEL_ID)) {
                modelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
            } else if (job.getOutputs() != null
                    && job.getOutputs().containsKey(WorkflowContextConstants.Inputs.MODEL_ID)) {
                modelId = job.getOutputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
            }
            if (modelId == null || !modelIdToModelSummaries.containsKey(modelId)) {
                continue;
            }

            if (modelIdToModelSummaries.get(modelId).getStatus() == ModelSummaryStatus.DELETED) {
                job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DELETED, "true");
            }
            job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME,
                    modelIdToModelSummaries.get(modelId).getDisplayName());
            job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_TYPE,
                    modelIdToModelSummaries.get(modelId).getModelType());
        }
    }

    @Override
    public ApplicationId submit(WorkflowConfiguration configuration) {
        String userId = MultiTenantContext.getEmailAddress();
        configuration.setUserId(userId);

        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);

        log.info(String.format("Submitted %s with application id %s",
                configuration.getWorkflowName(), applicationId));
        return ConverterUtils.toApplicationId(applicationId);
    }

    @Override
    public JobStatus getJobStatusFromApplicationId(String appId) {
        Job job = workflowProxy.getWorkflowJobFromApplicationId(appId);
        return job.getJobStatus();
    }

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        return tenantEntityMgr.findByTenantId(tenant.getId());
    }

    private Boolean getJobSourceFileExists(String applicationId) {
        if (applicationId == null || applicationId.isEmpty()) {
            return false;
        }

        SourceFile sourceFile = sourceFileEntityMgr.findByApplicationId(applicationId);
        if (sourceFile != null) {
            return true;
        }
        return false;
    }

}
