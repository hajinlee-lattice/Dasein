package com.latticeengines.pls.service.impl;

import java.text.SimpleDateFormat;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.JobStepDisplayInfoMapping;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
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
    private SourceFileProxy sourceFileProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

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
                updateJobWithRatingEngine(job);
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
        String tenantId = MultiTenantContext.getShortTenantId();
        return actionProxy.getActionsByPids(tenantId, actionPids);
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
    public List<Job> findByJobIds(List<String> jobIds, Boolean filterNonUiJobs, Boolean generateEmptyPAJob) {
        List<Job> jobs = workflowProxy.getWorkflowExecutionsByJobIds(jobIds,
                MultiTenantContext.getCustomerSpace().toString());
        if (jobs == null) {
            return Collections.emptyList();
        }
        return applyUiFriendlyFilters(jobs, filterNonUiJobs, generateEmptyPAJob);
    }

    @Override
    public List<Job> findAll(Boolean filterNonUiJobs, Boolean generateEmptyPAJob) {
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(MultiTenantContext.getTenant());
        if (jobs == null) {
            return Collections.emptyList();
        }
        return applyUiFriendlyFilters(jobs, filterNonUiJobs, generateEmptyPAJob);
    }

    private List<Job> applyUiFriendlyFilters(List<Job> jobs, Boolean filterNonUiJobs, Boolean generateEmptyPAJob) {
        if (jobs == null) {
            return Collections.emptyList();
        }

        if (filterNonUiJobs != null && filterNonUiJobs) {
            jobs.removeIf(job -> (job == null) || (job.getJobType() == null)
                    || (NON_DISPLAYED_JOB_TYPES.contains(job.getJobType().toLowerCase())));
        }
        updateAllJobs(jobs);

        if (generateEmptyPAJob != null && generateEmptyPAJob) {
            Job unstartedPnAJob = generateUnstartedProcessAnalyzeJob(true);
            if (unstartedPnAJob != null) {
                jobs.add(unstartedPnAJob);
            }
        }

        return jobs;
    }

    @Override
    public String generateCSVReport(String jobId) {
        Job job = this.find(jobId, true);
        if (job == null || job.getJobStatus() != JobStatus.COMPLETED
                || !job.getJobType().equals("processAnalyzeWorkflow")) {
            throw new LedpException(LedpCode.LEDP_18184);
        }
        List<Job> subjobs = job.getSubJobs();
        Report report = job.getReports().stream()
                .filter(r -> r.getPurpose() == ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY)
                .collect(Collectors.toList()).get(0);

        String columnDelimiter = ",";
        StringBuilder sb = new StringBuilder(
                "Summary Stats\n , Accounts, Contacts, Product Hierarchies, Product Bundles, Transactions \n");
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode jsonReport = (ObjectNode) om.readTree(report.getJson().getPayload());
            ObjectNode entitiesSummaryNode = (ObjectNode) jsonReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
            String[] reportConstants = { ReportConstants.NEW, ReportConstants.UPDATE, ReportConstants.DELETE,
                    ReportConstants.UNMATCH, "REPLACE" };
            BusinessEntity[] entities = { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Product,
                    BusinessEntity.Transaction };

            for (String reportConstant : reportConstants) {
                sb.append(StringUtils.capitalize(reportConstant.toLowerCase()) + columnDelimiter);
                for (BusinessEntity entity : entities) {
                    ObjectNode entityNode = (ObjectNode) entitiesSummaryNode.get(entity.name());
                    ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                            .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());

                    if (entity == BusinessEntity.Product) {
                        sb.append(reportConstant.equals("REPLACE")
                                ? consolidateSummaryNode.get(ReportConstants.PRODUCT_HIERARCHY).toString() : "NA");
                        sb.append(columnDelimiter);
                        sb.append(reportConstant.equals("REPLACE")
                                ? consolidateSummaryNode.get(ReportConstants.PRODUCT_BUNDLE).toString() : "NA");
                    } else {
                        sb.append(consolidateSummaryNode.get(reportConstant) != null
                                ? consolidateSummaryNode.get(reportConstant).toString() : "NA");
                    }
                    sb.append(columnDelimiter);
                }
                sb.append("\n");
            }

            sb.append("\n\n\nFiles Processed \n");
            sb.append("Start Time, Actions, Validation, Record Found, Record Failed, Record Uploaded, User \n");

            for (Job subjob : subjobs) {
                SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy h:mma");
                sb.append(df.format(subjob.getStartTimestamp()) + columnDelimiter);
                String sourceDisplayName = subjob.getInputs() != null
                        ? subjob.getInputs().get(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME) : null;
                if (sourceDisplayName != null) {
                    sb.append(
                            getActionType(subjob.getJobType()) + sourceDisplayName.replace(",", "-") + columnDelimiter);
                } else {
                    sb.append(subjob.getName() + columnDelimiter);
                }
                ObjectNode subjobPayload;
                if (subjob.getReports() != null) {
                    subjobPayload = (ObjectNode) om.readTree(subjob.getReports().get(0).getJson().getPayload());
                } else {
                    subjobPayload = null;
                }

                sb.append(getValidation(subjob.getJobStatus(), subjobPayload) + columnDelimiter);
                sb.append(getRecordsFound(subjobPayload, getImpactedBusinessEntity(subjob)) + columnDelimiter);
                sb.append(getPayloadValue(subjobPayload, "total_failed_rows") + columnDelimiter);
                sb.append(getPayloadValue(subjobPayload, "imported_rows") + columnDelimiter);
                sb.append(subjob.getUser());
                sb.append("\n");
            }

            if (jsonReport.has(ReportPurpose.SYSTEM_ACTIONS.getKey())) {
                sb.append("\n\n\nSystem Actions\n");
                ArrayNode systemActions = (ArrayNode) jsonReport.get(ReportPurpose.SYSTEM_ACTIONS.getKey());
                for (int i = 0; i < systemActions.size(); i++) {
                    sb.append(systemActions.get(i).toString() + "\n");
                }
            }

        } catch (Exception e) {
            log.error("Failed to generate report for job " + jobId);
        }
        return sb.toString();
    }

    private String getRecordsFound(ObjectNode payload, String impactedEntity) {
        if (payload != null && payload.has("total_rows")) {
            return getPayloadValue(payload, "total_rows");
        } else if (payload != null && payload.has(impactedEntity + "_Deleted")) {
            return getPayloadValue(payload, impactedEntity + "_Deleted");
        } else {
            return "-";
        }
    }

    private String getPayloadValue(ObjectNode subjobPayload, String key) {
        return subjobPayload != null && subjobPayload.has(key) ? subjobPayload.get(key).toString() : "-";
    }

    private String getImpactedBusinessEntity(Job job) {
        String str = job.getOutputs() != null
                ? job.getOutputs().get(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES) : "";
        if (StringUtils.isEmpty(str)) {
            return "";
        }
        List<?> entityList = JsonUtils.deserialize(str, List.class);
        return JsonUtils.convertList(entityList, BusinessEntity.class).get(0).name();
    }

    private String getValidation(JobStatus jobStatus, ObjectNode subjobPayload) {
        switch (jobStatus) {
        case PENDING:
        case RUNNING:
            return "In Progress";
        case COMPLETED:
            if (subjobPayload != null && subjobPayload.has("total_rows")
                    && subjobPayload.get("total_rows").asInt() != subjobPayload.get("imported_rows").asInt()) {
                return "Partial Success";
            } else {
                return "Success";
            }
        default:
            return jobStatus.getName();
        }
    }

    private String getActionType(String jobType) {
        switch (jobType) {
        case "cdlDataFeedImportWorkflow":
            return ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getDisplayName() + ": ";
        case "cdlOperationWorkflow":
            return ActionType.CDL_OPERATION_WORKFLOW.getDisplayName() + ": ";
        default:
            return jobType;
        }
    }

    @VisibleForTesting
    Job generateUnstartedProcessAnalyzeJob(boolean expandChildrenJobs) {
        Job job = new Job();
        job.setId(UNSTARTED_PROCESS_ANALYZE_ID);
        job.setName("processAnalyzeWorkflow");
        job.setJobStatus(JobStatus.READY);
        job.setJobType("processAnalyzeWorkflow");
        String tenantId = MultiTenantContext.getShortTenantId();
        List<Action> actions = actionProxy.getActionsByOwnerId(tenantId, null);
        updateStartTimeStampAndForJob(job);
        if (CollectionUtils.isNotEmpty(actions)) {
            Map<String, String> unfinishedInputContext = new HashMap<>();
            List<Long> unfinishedActionIds = actions.stream().filter(action -> isVisibleAction(action))
                    .map(Action::getPid).collect(Collectors.toList());
            unfinishedInputContext.put(WorkflowContextConstants.Inputs.ACTION_IDS, unfinishedActionIds.toString());
            job.setInputs(unfinishedInputContext);
            if (expandChildrenJobs) {
                job.setSubJobs(expandActions(actions));
            }
        }
        return job;
    }

    private Boolean isVisibleAction(Action action) {
        return action.getActionConfiguration() == null
                || (action.getActionConfiguration() != null && action.getActionConfiguration().isHiddenFromUI() != null
                        && !action.getActionConfiguration().isHiddenFromUI());
    }

    @VisibleForTesting
    void updateStartTimeStampAndForJob(Job job) {
        Date nextInvokeDate = new DateTime().plusDays(1).withTimeAtStartOfDay().toDate();
        boolean allowAutoSchedule = false;
        try {
            allowAutoSchedule = batonService.isEnabled(MultiTenantContext.getCustomerSpace(),
                    LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE);
        } catch (Exception e) {
            log.warn("get 'allow auto schedule' value failed: " + e.getMessage());
        }
        if (allowAutoSchedule) {
            try {
                Long nextInvokeTime = dataFeedProxy.nextInvokeTime(MultiTenantContext.getShortTenantId());
                if (nextInvokeTime != null && nextInvokeTime.compareTo(new DateTime().toDate().getTime()) > 0) {
                    nextInvokeDate = new Date(nextInvokeTime);
                } else if (nextInvokeTime != null && nextInvokeTime.compareTo(new DateTime().toDate().getTime()) < 0) {
                    DateTime previousInvokeTime = new DateTime(nextInvokeTime);
                    nextInvokeDate = new DateTime().plusDays(1).withTimeAtStartOfDay()
                            .plusHours(previousInvokeTime.getHourOfDay())
                            .plusMinutes(previousInvokeTime.getMinuteOfHour())
                            .plusSeconds(previousInvokeTime.getSecondOfMinute()).toDate();
                }
                // Only update note if auto schedule is on
                job.setNote(String.format(CDLNote, nextInvokeDate));
            } catch (Exception e) {
                log.warn(String.format("Geting next invoke time for tenant %s has error.",
                        MultiTenantContext.getShortTenantId()));
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
        List<String> workflowJobPids = new ArrayList<>();
        for (Action action : actions) {
            // this action is a workflow job
            if (action.getTrackingPid() != null) {
                workflowJobPids.add(action.getTrackingPid().toString());
            } else if (isVisibleAction(action)) {
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
        if (CollectionUtils.isNotEmpty(workflowJobPids)) {
            jobList.addAll(workflowProxy.getWorkflowExecutionsByJobPids(workflowJobPids,
                    MultiTenantContext.getCustomerSpace().toString()));
        }
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
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Map<String, ModelSummary> modelIdToModelSummaries = new HashMap<>();
        List<ModelSummary> modelSummaries = modelSummaryProxy.getModelSummaries(customerSpace.toString(), "all");
        for (ModelSummary modelSummary : modelSummaries) {
            modelIdToModelSummaries.put(modelSummary.getId(), modelSummary);
        }

        boolean hasCG = batonService.hasProduct(customerSpace, LatticeProduct.CG);
        Map<String, RatingEngineSummary> ratingIdToRatingEngineSummaries = new HashMap<>();
        if (hasCG) {
            List<RatingEngineSummary> ratingEngineSummaries = ratingEngineProxy
                    .getRatingEngineSummaries(MultiTenantContext.getTenant().getId());
            for (RatingEngineSummary ratingEngineSummary : ratingEngineSummaries) {
                ratingIdToRatingEngineSummaries.put(ratingEngineSummary.getId(), ratingEngineSummary);
            }
        }

        for (Job job : jobs) {
            updateStepDisplayNameAndNumSteps(job);
            updateJobDisplayNameAndDescription(job);
            updateJobWithModelSummaryInfo(job, true, modelIdToModelSummaries);
            if (hasCG) {
                updateJobWithRatingEngineSummaryInfo(job, true, ratingIdToRatingEngineSummaries);
            }
            updateJobWithSubJobsIfIsPnA(job);
        }
    }

    @VisibleForTesting
    void updateJobWithSubJobsIfIsPnA(Job job) {
        if (job.getJobType().equals("processAnalyzeWorkflow")) {
            List<Long> actionPids = getActionIdsForJob(job);
            if (CollectionUtils.isNotEmpty(actionPids)) {
                List<Action> actions = getActions(actionPids);
                List<Job> subJobs = expandActions(actions);
                job.setSubJobs(subJobs);
            }
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

    @VisibleForTesting
    void updateJobWithRatingEngine(Job job) {
        if (job.getInputs() == null) {
            job.setInputs(new HashMap<>());
        }
        updateJobWithRatingEngineSummaryInfo(job, false, Collections.emptyMap());
    }

    private void updateJobWithRatingEngineSummaryInfo(Job job, boolean useMap,
            Map<String, RatingEngineSummary> ratingIdToRatingEngineSummaries) {
        String ratingId = null;
        if (job.getJobType() != null && (job.getJobType().equals("customEventModelingWorkflow")
                || job.getJobType().equals("crossSellImportMatchAndModelWorkflow"))) {
            if (job.getInputs() != null
                    && job.getInputs().containsKey(WorkflowContextConstants.Inputs.RATING_ENGINE_ID)) {
                ratingId = job.getInputs().get(WorkflowContextConstants.Inputs.RATING_ENGINE_ID);
            }
            if (ratingId == null) {
                return;
            }
        } else {
            return;
        }

        String displayName = null;
        if (useMap) {
            if (ratingIdToRatingEngineSummaries.containsKey(ratingId)) {
                displayName = ratingIdToRatingEngineSummaries.get(ratingId).getDisplayName();
            }
        } else {
            RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(MultiTenantContext.getTenant().getId(),
                    ratingId);
            if (ratingEngine != null) {
                displayName = ratingEngine.getDisplayName();
            }
        }

        if (displayName != null) {
            job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, displayName);
        } else {
            log.warn(String.format("RatingEngine: %s for job: %s cannot be found in the database. Please check",
                    ratingId, job.getId()));
        }
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
            modelSummary = modelSummaryProxy.getByModelId(modelId);
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

        SourceFile sourceFile = sourceFileProxy.findByApplicationId(MultiTenantContext.getShortTenantId(),
                applicationId);
        return sourceFile != null;
    }

    @Override
    public List<Job> findJobs(List<String> jobIds, List<String> types, List<String> jobStatuses, Boolean includeDetails,
            Boolean hasParentId, Boolean filterNonUiJobs, Boolean generateEmptyPAJob) {
        List<Job> jobs = workflowProxy.getJobs(jobIds, types, jobStatuses, includeDetails,
                MultiTenantContext.getCustomerSpace().toString());
        if (jobs == null) {
            return Collections.emptyList();
        }
        return applyUiFriendlyFilters(jobs, filterNonUiJobs, generateEmptyPAJob);
    }
}
