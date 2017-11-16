package com.latticeengines.pls.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.JobStepDisplayInfoMapping;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
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

    private static final Logger log = LoggerFactory.getLogger(WorkflowJobService.class);
    private static final String[] NON_DISPLAYED_JOB_TYPE_VALUES = new String[] { //
            "bulkmatchworkflow", //
            "consolidateandpublishworkflow", //
            "profileandpublishworkflow" };
    private static final Set<String> NON_DISPLAYED_JOB_TYPES = new HashSet<>(
            Arrays.asList(NON_DISPLAYED_JOB_TYPE_VALUES));

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Override
    public ApplicationId restart(Long jobId) {
        AppSubmission submission = workflowProxy.restartWorkflowExecution(jobId);
        String applicationId = submission.getApplicationIds().get(0);

        log.info(String.format("Resubmitted workflow with application id %s", applicationId));
        return ConverterUtils.toApplicationId(applicationId);
    }

    @Override
    public void cancel(String jobId) {
        Tenant tenantWithPid = getTenant();
        workflowProxy.stopWorkflow(CustomerSpace.parse(tenantWithPid.getId()).toString(), jobId);
    }

    @Override
    public List<Job> findAllWithType(String type) {
        Tenant tenantWithPid = getTenant();
        log.debug("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid(), type);
        if (jobs == null) {
            jobs = Collections.emptyList();
        }
        for (Job job : jobs) {
            updateStepDisplayNameAndNumSteps(job);
            updateJobDisplayNameAndDescription(job);
        }
        return jobs;
    }

    @Override
    public Job findByApplicationId(String applicationId) {
        Tenant tenantWithPid = getTenant();
        log.debug("Finding job for application Id " + applicationId + " with pid " + tenantWithPid.getPid());
        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
        updateStepDisplayNameAndNumSteps(job);
        updateJobDisplayNameAndDescription(job);
        return job;
    }

    @Override
    public Job find(String jobId) {
        Job job = workflowProxy.getWorkflowExecution(jobId);
        updateJobWithModelSummary(job);
        updateStepDisplayNameAndNumSteps(job);
        updateJobDisplayNameAndDescription(job);
        return job;
    }

    @Override
    public List<Job> findByJobIds(List<String> jobIds) {
        List<Job> jobs = workflowProxy.getWorkflowExecutionsByJobIds(jobIds);
        if (jobs == null) {
            return Collections.emptyList();
        }
        return jobs;
    }

    @Override
    public List<Job> findAll() {
        Tenant tenantWithPid = getTenant();
        log.debug("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        if (jobs == null) {
            return Collections.emptyList();
        }

        jobs.removeIf(job -> NON_DISPLAYED_JOB_TYPES.contains(job.getJobType().toLowerCase()));
        updateAllJobsAndStepsWithModelSummaries(jobs);
        return jobs;
    }

    private void updateStepDisplayNameAndNumSteps(Job job) {
        if (job == null) {
            return;
        }

        List<JobStep> steps = job.getSteps();
        if (steps != null) {
            int count = 0;
            String previousStepDisplayName = "";
            for (int i = 0; i < steps.size(); i++) {
                JobStep step = steps.get(i);
                String stepDisplayName = JobStepDisplayInfoMapping.getMappedName(job.getJobType(), i);
                String stepDescription = JobStepDisplayInfoMapping.getMappedDescription(job.getJobType(), i);
                step.setName(stepDisplayName);
                step.setDescription(stepDescription);
                if (!stepDisplayName.equalsIgnoreCase(previousStepDisplayName)) {
                    count++;
                    previousStepDisplayName = stepDisplayName;
                }
            }
            job.setNumDisplayedSteps(count);
        }
    }

    private void updateJobDisplayNameAndDescription(Job job) {
        if (job == null) {
            return;
        }

        job.setName(job.getJobType());
        job.setDescription(job.getJobType());
    }

    private void updateAllJobsAndStepsWithModelSummaries(List<Job> jobs) {
        Map<String, ModelSummary> modelIdToModelSummaries = new HashMap<>();
        List<ModelSummary> modelSummaries = modelSummaryService.getModelSummaries("all");
        for (ModelSummary modelSummary : modelSummaries) {
            modelIdToModelSummaries.put(modelSummary.getId(), modelSummary);
        }

        for (Job job : jobs) {
            updateStepDisplayNameAndNumSteps(job);
            updateJobDisplayNameAndDescription(job);
            String modelId = null;
            if (job.getInputs() != null && job.getInputs().containsKey(WorkflowContextConstants.Inputs.MODEL_ID)) {
                modelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
            } else if (job.getOutputs() != null
                    && job.getOutputs().containsKey(WorkflowContextConstants.Inputs.MODEL_ID)) {
                modelId = job.getOutputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
            }
            if (modelId == null) {
                continue;
            }

            ModelSummary modelSummary = modelIdToModelSummaries.get(modelId);
            if (modelSummary != null) {
                if (modelSummary.getStatus() == ModelSummaryStatus.DELETED) {
                    job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DELETED, "true");
                }
                job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, modelSummary.getDisplayName());
                job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_TYPE, modelSummary.getModelType());
            } else {
                log.warn(String.format("ModelSummary: %s for job: %s cannot be found in the database. Please check",
                        modelId, job.getId()));
                job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DELETED, "true");
            }
        }
    }

    private void updateJobWithModelSummary(Job job) {
        if (job.getInputs() == null) {
            job.setInputs(new HashMap<String, String>());
        }
        job.getInputs().put(WorkflowContextConstants.Inputs.SOURCE_FILE_EXISTS,
                getJobSourceFileExists(job.getApplicationId()).toString());

        String modelId = null;
        if (job.getInputs() != null && job.getInputs().containsKey(WorkflowContextConstants.Inputs.MODEL_ID)) {
            modelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
        } else if (job.getOutputs() != null && job.getOutputs().containsKey(WorkflowContextConstants.Inputs.MODEL_ID)) {
            modelId = job.getOutputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
        }
        if (modelId == null) {
            return;
        }

        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary != null) {
            if (modelSummary.getStatus() == ModelSummaryStatus.DELETED) {
                job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DELETED, "true");
            }
            job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, modelSummary.getDisplayName());
            job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_TYPE, modelSummary.getModelType());
        } else {
            log.warn(String.format("ModelSummary: %s for job: %s cannot be found in the database. Please check",
                    modelId, job.getId()));
            job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DELETED, "true");
        }
    }

    @Override
    public ApplicationId submit(WorkflowConfiguration configuration) {
        String userId = MultiTenantContext.getEmailAddress();
        configuration.setUserId(userId);

        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);

        log.info(String.format("Submitted %s with application id %s", configuration.getWorkflowName(), applicationId));
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

    @Override
    public List<Job> findJobs(List<String> jobIds, List<String> types, Boolean includeDetails, Boolean hasParentId) {
        Tenant tenantWithPid = getTenant();
        List<Job> jobs = workflowProxy.getWorkflowJobs(CustomerSpace.parse(tenantWithPid.getId()).toString(), jobIds,
                types, includeDetails, hasParentId);
        if (jobs == null) {
            return Collections.emptyList();
        }
        return jobs;
    }

}
