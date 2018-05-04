package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.JobStepDisplayInfoMapping;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("workflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {

    public static final String CDLNote = "Scheduled at %s.";
    // Special workflow id for unstarted ProcessAnalyze workflow
    public static final Long UNSTARTED_PROCESS_ANALYZE_ID = 0L;

    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImpl.class);
    private static final String[] NON_DISPLAYED_JOB_TYPE_VALUES = new String[] { //
            "bulkmatchworkflow", //
            "consolidateandpublishworkflow", //
            "profileandpublishworkflow" };
    private static final Set<String> NON_DISPLAYED_JOB_TYPES = new HashSet<>(
            Arrays.asList(NON_DISPLAYED_JOB_TYPE_VALUES));

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private ActionService actionService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private BatonService batonService;

    @Override
    public ApplicationId restart(Long jobId) {
        AppSubmission submission = workflowProxy.restartWorkflowExecution(String.valueOf(jobId),
                MultiTenantContext.getCustomerSpace().toString());
        String applicationId = submission.getApplicationIds().get(0);

        log.info(String.format("Resubmitted workflow with application id %s", applicationId));
        return ConverterUtils.toApplicationId(applicationId);
    }

    @Override
    public void cancel(String jobId) {
        workflowProxy.stopWorkflowExecution(jobId, MultiTenantContext.getCustomerSpace().toString());
    }

    @Override
    public List<Job> findAllWithType(String type) {
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(MultiTenantContext.getTenant(), type);
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
        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId,
                MultiTenantContext.getCustomerSpace().toString());
        updateStepDisplayNameAndNumSteps(job);
        updateJobDisplayNameAndDescription(job);
        return job;
    }

    @Override
    public Job find(String jobId, boolean useCustomerSpace) {
        if (jobId == null) {
            throw new NullPointerException("jobId cannot be null.");
        }
        Job job = null;
        String customerSpace = null;
        // For unfinished ProcessAnalyze job
        if (Long.parseLong(jobId) == UNSTARTED_PROCESS_ANALYZE_ID) {
            return generateUnstartedProcessAnalyzeJob(true);
        } else {
            if (useCustomerSpace) {
                customerSpace = MultiTenantContext.getCustomerSpace().toString();
                log.info("Getting job with id=" + jobId + ", customerSpace=" + customerSpace);
                job = workflowProxy.getWorkflowExecution(jobId, customerSpace);
            } else {
                log.info("Getting job with id=" + jobId);
                job = workflowProxy.getWorkflowExecution(jobId);
            }

            if (job != null) {
                updateJobWithModelSummary(job);
                updateStepDisplayNameAndNumSteps(job);
                updateJobDisplayNameAndDescription(job);
                updateJobWithSubJobsIfIsPnA(job);
            } else {
                if (useCustomerSpace) {
                    log.error(String.format("Job of jobId=%s is null for customerSpace=%s", jobId, customerSpace));
                } else {
                    log.error(String.format("Job of jobId=%s is null", jobId));
                }
            }
        }

        return job;
    }

    List<Action> getActions(List<Long> actionPids) {
        return actionService.findByPidIn(actionPids);
    }

    @Override
    public List<Job> findJobsBasedOnActionIdsAndType(List<Long> actionPids, ActionType actionType) {
        if (CollectionUtils.isEmpty(actionPids)) {
            return Collections.emptyList();
        }
        List<Action> actionsWithType = getActions(actionPids).stream()
                .filter(action -> action.getType().equals(actionType)).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(actionsWithType)) {
            return expandActions(actionsWithType);
        }
        return Collections.emptyList();
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    List<Long> getActionIdsForJob(Job job) {
        String actionIdsStr = job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_IDS);
        if (StringUtils.isEmpty(actionIdsStr)) {
            throw new LedpException(LedpCode.LEDP_18172, new String[] { job.getId().toString() });
        }
        List<Object> listObj = JsonUtils.deserialize(actionIdsStr, List.class);
        return JsonUtils.convertList(listObj, Long.class);
    }

    @Override
    public List<Job> findByJobIds(List<String> jobIds) {
        List<Job> jobs = workflowProxy.getWorkflowExecutionsByJobIds(jobIds,
                MultiTenantContext.getCustomerSpace().toString());
        if (jobs == null) {
            return Collections.emptyList();
        }
        return jobs;
    }

    @Override
    public List<Job> findAll() {
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(MultiTenantContext.getTenant());
        if (jobs == null) {
            return Collections.emptyList();
        }

        jobs.removeIf(job -> (job == null) || (job.getJobType() == null)
                || (NON_DISPLAYED_JOB_TYPES.contains(job.getJobType().toLowerCase())));
        updateAllJobs(jobs);
        Job unstartedPnAJob = generateUnstartedProcessAnalyzeJob(true);
        if (unstartedPnAJob != null) {
            jobs.add(unstartedPnAJob);
        }
        return jobs;
    }

    @VisibleForTesting
    Job generateUnstartedProcessAnalyzeJob(boolean expandChildrenJobs) {
        Job job = null;
        List<Action> actions = actionService.findByOwnerId(null, null);
        if (CollectionUtils.isNotEmpty(actions)) {
            job = new Job();
            job.setId(UNSTARTED_PROCESS_ANALYZE_ID);
            job.setName("processAnalyzeWorkflow");
            job.setJobStatus(JobStatus.READY);
            job.setJobType("processAnalyzeWorkflow");
            Map<String, String> unfinishedInputContext = new HashMap<>();
            List<Long> unfinishedActionIds = actions.stream().map(Action::getPid).collect(Collectors.toList());
            unfinishedInputContext.put(WorkflowContextConstants.Inputs.ACTION_IDS, unfinishedActionIds.toString());
            job.setInputs(unfinishedInputContext);
            updateStartTimeStampAndForJob(job);
            if (expandChildrenJobs) {
                job.setSubJobs(expandActions(actions));
            }
        }
        return job;
    }

    private void updateStartTimeStampAndForJob(Job job) {
        Date nextInvokeDate = new DateTime().plusDays(1).toDate();
        boolean allowAutoSchedule = false;
        try {
            allowAutoSchedule = batonService.isEnabled(CustomerSpace.parse(MultiTenantContext.getTenant().getId()),
                    LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
        } catch (Exception e) {
            log.warn("get 'allow auto schedule' value failed: " + e.getMessage());
        }
        if (allowAutoSchedule) {
            try {
                Long nextInvokeTime = dataFeedProxy.nextInvokeTime(MultiTenantContext.getTenantId());
                if (nextInvokeTime != null) {
                    nextInvokeDate = new Date(nextInvokeTime);
                }
                // Only update note if auto schedule is on
                job.setNote(String.format(CDLNote, nextInvokeDate));
            } catch (Exception e) {
                log.warn(String.format("Geting next invoke time for tenant %s has error.",
                        MultiTenantContext.getTenantId()));
            }
        }
        job.setStartTimestamp(nextInvokeDate);
    }

    @VisibleForTesting
    List<Job> expandActions(List<Action> actions) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Expand actions...actions=%s", actions));
        }

        log.info(String.format("Expanding %d actions", actions.size()));

        List<Job> jobList = new ArrayList<>();
        List<String> workflowJobIds = new ArrayList<>();
        for (Action action : actions) {
            // this action is a workflow job
            if (action.getTrackingId() != null) {
                workflowJobIds.add(action.getTrackingId().toString());
            } else {
                Job job = new Job();
                job.setName(action.getType().getDisplayName());
                job.setJobType(action.getType().getName());
                job.setUser(action.getActionInitiator());
                job.setStartTimestamp(action.getCreated());
                job.setDescription(action.getDescription());
                if (!ActionType.getNonWorkflowActions().contains(action.getType())) {
                    job.setJobStatus(JobStatus.RUNNING);
                } else {
                    job.setJobStatus(JobStatus.COMPLETED);
                }
                jobList.add(job);
            }
        }
        jobList.addAll(workflowProxy.getWorkflowExecutionsByJobIds(workflowJobIds,
                MultiTenantContext.getCustomerSpace().toString()));
        return jobList;
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

    private void updateAllJobs(List<Job> jobs) {
        Map<String, ModelSummary> modelIdToModelSummaries = new HashMap<>();
        List<ModelSummary> modelSummaries = modelSummaryService.getModelSummaries("all");
        for (ModelSummary modelSummary : modelSummaries) {
            modelIdToModelSummaries.put(modelSummary.getId(), modelSummary);
        }

        for (Job job : jobs) {
            updateStepDisplayNameAndNumSteps(job);
            updateJobDisplayNameAndDescription(job);
            updateJobWithModelSummaryInfo(job, true, modelIdToModelSummaries);
            updateJobWithSubJobsIfIsPnA(job);
        }
    }

    @VisibleForTesting
    void updateJobWithSubJobsIfIsPnA(Job job) {
        if (job.getJobType().equals("processAnalyzeWorkflow")) {
            List<Long> actionPids = getActionIdsForJob(job);
            List<Action> actions = getActions(actionPids);
            List<Job> subJobs = expandActions(actions);
            job.setSubJobs(subJobs);
        }
    }

    private void updateJobWithModelSummary(Job job) {
        if (job.getInputs() == null) {
            job.setInputs(new HashMap<>());
        }
        job.getInputs().put(WorkflowContextConstants.Inputs.SOURCE_FILE_EXISTS,
                getJobSourceFileExists(job.getApplicationId()).toString());

        updateJobWithModelSummaryInfo(job, false, Collections.emptyMap());
    }

    private void updateJobWithModelSummaryInfo(Job job, boolean useMap,
            Map<String, ModelSummary> modelIdToModelSummaries) {
        String modelId = null;
        if (job.getInputs() != null && job.getInputs().containsKey(WorkflowContextConstants.Inputs.MODEL_ID)) {
            modelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
        } else if (job.getOutputs() != null && job.getOutputs().containsKey(WorkflowContextConstants.Inputs.MODEL_ID)) {
            modelId = job.getOutputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
        }
        if (modelId == null) {
            return;
        }

        ModelSummary modelSummary = null;
        if (useMap) {
            modelSummary = modelIdToModelSummaries.get(modelId);
        } else {
            modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        }

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

        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration,
                MultiTenantContext.getCustomerSpace().toString());
        String applicationId = submission.getApplicationIds().get(0);

        log.info(String.format("Submitted %s with application id %s", configuration.getWorkflowName(), applicationId));
        return ConverterUtils.toApplicationId(applicationId);
    }

    @Override
    public JobStatus getJobStatusFromApplicationId(String appId) {
        Job job = workflowProxy.getWorkflowJobFromApplicationId(appId,
                MultiTenantContext.getCustomerSpace().toString());
        return job.getJobStatus();
    }

    private Boolean getJobSourceFileExists(String applicationId) {
        if (applicationId == null || applicationId.isEmpty()) {
            return false;
        }

        SourceFile sourceFile = sourceFileEntityMgr.findByApplicationId(applicationId);
        return sourceFile != null;
    }

    @Override
    public List<Job> findJobs(List<String> jobIds, List<String> types, Boolean includeDetails, Boolean hasParentId) {
        List<Job> jobs = workflowProxy.getJobs(jobIds, types, includeDetails,
                MultiTenantContext.getCustomerSpace().toString());
        if (jobs == null) {
            return Collections.emptyList();
        }
        return jobs;
    }
}
