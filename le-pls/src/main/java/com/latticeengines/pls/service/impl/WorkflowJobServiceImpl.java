package com.latticeengines.pls.service.impl;

import static com.latticeengines.domain.exposed.pls.ActionType.HARD_DELETE;
import static com.latticeengines.domain.exposed.pls.ActionType.SOFT_DELETE;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingPAUtils;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingStatus;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.FirstActionTimePending;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.LastActionTimePending;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.JobStepDisplayInfoMapping;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.JobStep;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("workflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImpl.class);
    static final String PA_JOB_TYPE = "processAnalyzeWorkflow";
    static final String ORPHAN_JOB_TYPE = "orphanRecordsExportWorkflow";
    static final String ORPHAN_ARTIFACT_EXPIRED = "ORPHAN_ARTIFACT_EXPIRED";

    private static final String SCHEDULED_MESSAGE = "Scheduled";
    public static final String CDLNote = "Scheduled at %s.";
    // Special workflow id for unstarted ProcessAnalyze workflow
    public static final Long UNSTARTED_PROCESS_ANALYZE_ID = 0L;
    private static final String[] NON_DISPLAYED_JOB_TYPE_VALUES = new String[]{ //
            "bulkmatchworkflow", //
            "consolidateandpublishworkflow", //
            "profileandpublishworkflow", //
            "timelineexportworkflow", //
            "generateintentemailalertworkflow", //
            "campaigndeltacalculationworkflow", //
            "deltacampaignlaunchworkflow", //
            "importlistsegmentworkflow"};
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

    @Inject
    private CDLProxy cdlProxy;

    @Value("${workflow.jobs.pa.hideRetried}")
    private boolean hideRetriedPA;

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
        List<Job> jobs = workflowProxy.getJobs(null, Arrays.asList(type), null, MultiTenantContext.getShortTenantId());
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
            return generateUnstartedProcessAnalyzeJob(true, getSchedulingStatus(MultiTenantContext.getCustomerSpace()));
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
                updateJobWithSubJobsIfIsPnA(job, null, null, null);
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

    @VisibleForTesting
    List<Action> getActions(List<Long> actionPids, Map<Long, Action> actionMap) {
        if (MapUtils.isNotEmpty(actionMap)) {
            List<Action> actions = new ArrayList<>();
            for (Long actionPid : actionPids) {
                Action action = actionMap.get(actionPid);
                if (action != null) {
                    actions.add(actionMap.get(actionPid));
                }
            }
            return actions;
        } else {
            String tenantId = MultiTenantContext.getShortTenantId();
            return actionProxy.getActionsByPids(tenantId, actionPids);
        }
    }

    @Override
    public List<Job> findJobsBasedOnActionIdsAndType(List<Long> actionPids, ActionType actionType) {
        if (CollectionUtils.isEmpty(actionPids)) {
            return Collections.emptyList();
        }
        List<Action> actionsWithType = getActions(actionPids, null).stream()
                .filter(action -> action.getType().equals(actionType)).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(actionsWithType)) {
            return expandActions(actionsWithType, null);
        }
        return Collections.emptyList();
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    List<Long> getActionIdsForJob(Job job) {
        String actionIdsStr = job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_IDS);
        if (StringUtils.isEmpty(actionIdsStr)) {
            throw new LedpException(LedpCode.LEDP_18172, new String[]{job.getId().toString()});
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
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(MultiTenantContext.getTenant(), true);
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
        SchedulingStatus schedulingStatus = getSchedulingStatus(MultiTenantContext.getCustomerSpace());
        updateAllJobs(jobs);
        if (generateEmptyPAJob != null && generateEmptyPAJob) {
            Job unstartedPnAJob = generateUnstartedProcessAnalyzeJob(true, schedulingStatus);
            if (unstartedPnAJob != null) {
                jobs.add(unstartedPnAJob);
            }
        }
        try {
            updateRetriedPASteps(jobs);
        } catch (Exception e) {
            log.error(String.format("Failed to update retried PA jobs steps for tenant %s",
                    MultiTenantContext.getCustomerSpace()), e);
        }
        try {
            updatePendingRetryJobs(jobs, schedulingStatus);
        } catch (Exception e) {
            log.error(String.format("Failed to update pending retry PA job status for tenant %s",
                    MultiTenantContext.getCustomerSpace()), e);
        }
        // not showing retried PA
        return jobs.stream() //
                .filter(job -> !JobStatus.RETRIED.equals(job.getJobStatus())) //
                .collect(Collectors.toList());
    }

    private void updateRetriedPASteps(List<Job> jobs) {
        if (!hideRetriedPA) {
            return;
        }

        // job.getId (workflow id) -> job
        Map<Long, Job> jobMap = jobs.stream() //
                .filter(Objects::nonNull) //
                .filter(job -> job.getId() != null) //
                .collect(Collectors.toMap(Job::getId, job -> job, (v1, v2) -> v1));

        jobs.stream() //
                .filter(Objects::nonNull) //
                .filter(job -> {
                    Long id = getRestartedJobId(job);
                    return id != null && jobMap.containsKey(id);
                }) //
                .map(job -> Pair.of(job, jobMap.get(getRestartedJobId(job)))) //
                .filter(pair -> pair.getLeft() != null && pair.getRight() != null) //
                .forEach(pair -> {
                    Job job = pair.getLeft();
                    Job retriedJob = pair.getRight();
                    if (retriedJob == null || retriedJob.getJobStatus() != JobStatus.RETRIED) {
                        return;
                    }
                    if (job.getJobStatus() == JobStatus.PENDING) {
                        // make it as if old job is still running
                        job.setJobStatus(JobStatus.RUNNING);
                    }
                    // when job is running, set the flag
                    // since UI will display a different tooltip
                    if (job.isRunning()) {
                        job.setJobRetried(true);
                    }

                    if (CollectionUtils.isNotEmpty(retriedJob.getSteps())) {
                        // merge steps, use restarted job's step first
                        List<JobStep> mergedSteps = new ArrayList<>();
                        // fake start timestamp as well
                        if (retriedJob.getStartTimestamp() != null) {
                            job.setStartTimestamp(retriedJob.getStartTimestamp());
                        }

                        int m = CollectionUtils.size(job.getSteps());
                        int n = retriedJob.getSteps().size();

                        for (int i = 0; i < Math.max(m, n); i++) {
                            if (i < n && retriedJob.getSteps().get(i) != null
                                    && retriedJob.getSteps().get(i).getStepStatus() != JobStatus.FAILED) {
                                // use the failed job's step first to make it look like it's still running (not
                                // failed), skip the failed step
                                mergedSteps.add(retriedJob.getSteps().get(i));
                            } else if (i < m) {
                                mergedSteps.add(job.getSteps().get(i));
                            }
                        }
                        if (!mergedSteps.isEmpty() && job.getJobStatus() == JobStatus.RUNNING) {
                            // make last step running
                            mergedSteps.get(mergedSteps.size() - 1).setStepStatus(JobStatus.RUNNING);
                        }

                        job.setSteps(mergedSteps);
                    }
                });
    }

    private Long getRestartedJobId(@NotNull Job retryJob) {
        String retryJobId = MapUtils.emptyIfNull(retryJob.getInputs())
                .get(WorkflowContextConstants.Inputs.RESTART_JOB_ID);
        if (StringUtils.isBlank(retryJobId)) {
            return null;
        }
        try {
            return Long.parseLong(retryJobId);
        } catch (Exception e) {
            // do nothing
            return null;
        }
    }

    /*
     * Update all jobs with Pending Retry status: 1. last failed one, if waiting for
     * scheduler retry => RUNNING 2. others => FAILED
     */
    private void updatePendingRetryJobs(List<Job> jobs, SchedulingStatus status) {
        if (CollectionUtils.isEmpty(jobs)) {
            return;
        }
        Long pendingRetryPAId = getPendingRetryPAId(status);

        jobs.stream() //
                .filter(Objects::nonNull) //
                .filter(job -> job.getJobStatus() == JobStatus.PENDING_RETRY) //
                .forEach(job -> {
                    if (hideRetriedPA && pendingRetryPAId != null && pendingRetryPAId.equals(job.getId())) {
                        job.setJobStatus(JobStatus.RUNNING);
                        // set the flag when job can but not yet retried
                        job.setJobRetried(true);
                        if (CollectionUtils.isNotEmpty(job.getSteps())) {
                            List<JobStep> steps = job.getSteps();
                            JobStep lastStep = steps.get(steps.size() - 1);
                            // make it seems like it's still running
                            if (lastStep != null && lastStep.getStepStatus() == JobStatus.FAILED) {
                                lastStep.setStepStatus(JobStatus.RUNNING);
                            }
                        }
                    } else {
                        job.setJobStatus(JobStatus.FAILED);
                    }
                });

        if (!hideRetriedPA) {
            // set retried status to failed
            jobs.stream() //
                    .filter(Objects::nonNull) //
                    .filter(job -> job.getJobStatus() == JobStatus.RETRIED) //
                    .forEach(job -> job.setJobStatus(JobStatus.FAILED));
        }
    }

    /*-
     * get workflow pid for last failed PA
     */
    private Long getPendingRetryPAId(SchedulingStatus status) {
        // check scheduler is enabled and all references exist
        if (status == null) {
            return null;
        }
        // TODO consider only return when scheduler is enabled (active stack)
        // - status.isSchedulerEnabled()
        if (status.getDataFeed() == null || status.getLatestExecution() == null
                || status.getLatestExecution().getStatus() != DataFeedExecution.Status.Failed) {
            return null;
        }

        DataFeedExecution exec = status.getLatestExecution();
        if (exec.getWorkflowId() == null || !status.isPendingRetry()) {
            if (exec.getWorkflowId() == null) {
                log.warn("Got empty workflowId for last failed execution. ExecutionId={}, tenant={}", exec.getPid(),
                        status.getCustomerSpace());
            }
            return null;
        }
        return exec.getWorkflowId();
    }

    @Override
    public String generateCSVReport(String jobId) {
        Job job = this.find(jobId, true);
        if (job == null || job.getJobStatus() != JobStatus.COMPLETED
                || !job.getJobType().equals(PA_JOB_TYPE)) {
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
            String[] reportConstants = {ReportConstants.NEW, ReportConstants.UPDATE, ReportConstants.DELETE,
                    ReportConstants.UNMATCH, "REPLACE"};
            BusinessEntity[] entities = {BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Product,
                    BusinessEntity.Transaction};

            for (String reportConstant : reportConstants) {
                sb.append(StringUtils.capitalize(reportConstant.toLowerCase())).append(columnDelimiter);
                for (BusinessEntity entity : entities) {
                    ObjectNode entityNode = (ObjectNode) entitiesSummaryNode.get(entity.name());
                    ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                            .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());

                    if (entity == BusinessEntity.Product) {
                        sb.append("REPLACE".equals(reportConstant)
                                ? consolidateSummaryNode.get(ReportConstants.PRODUCT_HIERARCHY).toString() : "NA");
                        sb.append(columnDelimiter);
                        sb.append("REPLACE".equals(reportConstant)
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
                sb.append(df.format(subjob.getStartTimestamp())).append(columnDelimiter);
                String sourceDisplayName = subjob.getInputs() != null
                        ? subjob.getInputs().get(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME) : null;
                if (sourceDisplayName != null) {
                    sb.append(getActionType(subjob.getJobType()))
                            .append(sourceDisplayName.replace(",", "-"))
                            .append(columnDelimiter);
                } else {
                    sb.append(subjob.getName()).append(columnDelimiter);
                }
                ObjectNode subjobPayload;
                if (subjob.getReports() != null) {
                    subjobPayload = (ObjectNode) om.readTree(subjob.getReports().get(0).getJson().getPayload());
                } else {
                    subjobPayload = null;
                }

                sb.append(getValidation(subjob.getJobStatus(), subjobPayload)).append(columnDelimiter);
                sb.append(getRecordsFound(subjobPayload, getImpactedBusinessEntity(subjob))).append(columnDelimiter);
                sb.append(getPayloadValue(subjobPayload, "total_failed_rows")).append(columnDelimiter);
                sb.append(getPayloadValue(subjobPayload, "imported_rows")).append(columnDelimiter);
                sb.append(subjob.getUser());
                sb.append("\n");
            }

            if (jsonReport.has(ReportPurpose.SYSTEM_ACTIONS.getKey())) {
                sb.append("\n\n\nSystem Actions\n");
                ArrayNode systemActions = (ArrayNode) jsonReport.get(ReportPurpose.SYSTEM_ACTIONS.getKey());
                for (int i = 0; i < systemActions.size(); i++) {
                    sb.append(systemActions.get(i).toString()).append("\n");
                }
            }

        } catch (Exception e) {
            log.error("Failed to generate report for job " + jobId);
        }
        return sb.toString();
    }

    @Override
    public void setErrorCategoryByJobPid(String jobPid, String errorCategory) {
        workflowProxy.setErrorCategoryByJobPid(jobPid, errorCategory, MultiTenantContext.getCustomerSpace().toString());
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
    Job generateUnstartedProcessAnalyzeJob(boolean expandChildrenJobs, SchedulingStatus schedulingStatus) {
        Job job = new Job();
        job.setId(UNSTARTED_PROCESS_ANALYZE_ID);
        job.setName(PA_JOB_TYPE);
        job.setJobStatus(JobStatus.READY);
        job.setJobType(PA_JOB_TYPE);
        String tenantId = MultiTenantContext.getShortTenantId();
        List<Action> actions = actionProxy.getActionsByOwnerId(tenantId, null);
        updateStartTimeStampAndForJob(job);
        setSchedulingInfo(job, MultiTenantContext.getCustomerSpace(), schedulingStatus);
        if (CollectionUtils.isNotEmpty(actions)) {
            Map<String, String> unfinishedInputContext = new HashMap<>();
            List<Long> unfinishedActionIds = actions.stream()
                    .filter(this::isVisibleAction)
                    .map(Action::getPid).collect(Collectors.toList());
            unfinishedInputContext.put(WorkflowContextConstants.Inputs.ACTION_IDS, unfinishedActionIds.toString());
            job.setInputs(unfinishedInputContext);
            if (expandChildrenJobs) {
                job.setSubJobs(updateJobsWithActionId(expandActions(actions, null), null));
            }
            updateEstimateScheduleTime(job, schedulingStatus, actions);
        }
        return job;
    }

    private SchedulingStatus getSchedulingStatus(CustomerSpace customerSpace) {
        if (customerSpace == null) {
            log.warn("Customerspace should not be null");
            return null;
        }

        return cdlProxy.getSchedulingStatus(customerSpace.toString());
    }

    /*
     * Set scheduling info for unstarted PA job
     */
    private void setSchedulingInfo(Job job, CustomerSpace customerSpace, SchedulingStatus schedulingStatus) {
        if (schedulingStatus == null) {
            log.error("Failed to get scheduling status for tenant = {}", customerSpace.toString());
            return;
        }

        // construct scheduling info
        boolean scheduleNowClicked = false;
        if (schedulingStatus.getDataFeed() != null) {
            scheduleNowClicked = Boolean.TRUE.equals(schedulingStatus.getDataFeed().isScheduleNow());
        }
        boolean paRunning = schedulingStatus.getDataFeed() != null
                && schedulingStatus.getDataFeed().getStatus() == DataFeed.Status.ProcessAnalyzing;
        boolean hasScheduleNowQuota = MapUtils.emptyIfNull(schedulingStatus.getRemainingPaQuota())
                .getOrDefault(SchedulerConstants.QUOTA_SCHEDULE_NOW, 0L) > 0;
        log.info("Tenant = {}, schedulerEnabled = {}, scheduleNowClicked = {}, paRunning = {}, remainingPaQuota = {}",
                customerSpace.toString(), schedulingStatus.isSchedulerEnabled(), scheduleNowClicked, paRunning,
                schedulingStatus.getRemainingPaQuota());
        // currently only consider schedule now & pa not running
        boolean tenantScheduled = schedulingStatus.isSchedulerEnabled() && scheduleNowClicked && !paRunning;
        job.setSchedulingInfo(
                new Job.SchedulingInfo(schedulingStatus.isSchedulerEnabled(), tenantScheduled, hasScheduleNowQuota));

        // change the job status for scheduled tenant (when job is running, status
        // should be ready)
        if (tenantScheduled) {
            job.setJobStatus(JobStatus.PENDING);
            job.getSchedulingInfo().setMessage(SCHEDULED_MESSAGE);
        }
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

    /*
     * Update estimate invoke time for autoscheduling 1) 2 hours after last import
     * job 2) check if in peace period 3)start at next day if no auto schedule quota
     */
    private void updateEstimateScheduleTime(Job job, SchedulingStatus schedulingStatus, List<Action> actions) {
        boolean allowAutoSchedule = false;
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        try {
            allowAutoSchedule = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ALLOW_AUTO_SCHEDULE) && !schedulingStatus.isHandHoldPATenant();
        } catch (Exception e) {
            log.warn("get 'allow auto schedule' value failed: " + e.getMessage());
        }
        if (allowAutoSchedule) {
            try {
                Set<Long> runningJobSet = job.getSubJobs().stream().filter(subJob -> subJob.isRunning())
                        .map(Job::getPid).collect(Collectors.toSet());
                actions = actions.stream().filter(action -> !runningJobSet.contains(action.getTrackingPid()))
                        .collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(actions)) {
                    ZoneId timezone = batonService.getTenantTimezone(customerSpace);
                    long autoScheduleQuota = MapUtils.emptyIfNull(schedulingStatus.getRemainingPaQuota())
                            .getOrDefault(SchedulerConstants.QUOTA_AUTO_SCHEDULE, 0L);
                    Instant now = Instant.now();
                    Instant estimateStartTime = now;

                    Action firstImportAction = Collections.min(actions, Comparator.comparing(Action::getCreated));
                    if (firstImportAction != null && firstImportAction.getCreated() != null) {
                        Instant importTime = firstImportAction.getCreated().toInstant()
                                .plus(FirstActionTimePending.FIRSTACTION_PENDING_HOUR, ChronoUnit.HOURS);
                        estimateStartTime = getLatestInstant(estimateStartTime, importTime);
                    }
                    Action lastImportAction = Collections.max(actions, Comparator.comparing(Action::getCreated));
                    if (lastImportAction != null && lastImportAction.getCreated() != null) {
                        Instant importTime = lastImportAction.getCreated().toInstant()
                                .plus(LastActionTimePending.LASTACTION_PENDING_MINITE, ChronoUnit.MINUTES);
                        estimateStartTime = getLatestInstant(estimateStartTime, importTime);
                    }

                    if (SchedulingPAUtils.isInPeacePeriod(estimateStartTime, timezone, autoScheduleQuota)) {
                        Pair<Instant, Instant> period = SchedulingPAUtils.calculatePeacePeriod(estimateStartTime,
                                timezone, autoScheduleQuota);
                        if (period != null) {
                            estimateStartTime = period.getRight();
                        }
                    }

                    if (autoScheduleQuota <= 0) {
                        Instant quotaTime = now.atZone(timezone).plus(1L, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS)
                                .toInstant();
                        estimateStartTime = getLatestInstant(quotaTime, estimateStartTime);
                    }

                    // update only when schedule time is not now
                    if (job.getSchedulingInfo() != null && estimateStartTime.isAfter(now)) {
                        job.getSchedulingInfo().setEstimateTimestamp(Date.from(estimateStartTime));
                    }
                    log.info("estimate auto schedule time for for tenant {} is {} ",
                            MultiTenantContext.getShortTenantId(), estimateStartTime.toString());
                }
            } catch (Exception e) {
                log.warn(String.format("Getting estimate schedule time for tenant %s has error",
                        MultiTenantContext.getShortTenantId()), e);
            }
        }
    }

    private Instant getLatestInstant(Instant instant1, Instant instant2) {
        return instant1.isAfter(instant2) ? instant1 : instant2;
    }

    @VisibleForTesting
    List<Job> expandActions(List<Action> actions, Map<String, Job> jobMap) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Expand actions=%s", JsonUtils.serialize(actions)));
        }
        log.debug(String.format("Expanding %d actions", actions.size()));

        List<Job> jobList = new ArrayList<>();
        List<String> canceled_workflowJobPids = new ArrayList<>();
        List<String> workflowJobPids = new ArrayList<>();
        for (Action action : actions) {
            // this action is a workflow job
            if (action.getTrackingPid() != null) {
                if (action.getType() == ActionType.CDL_DATAFEED_IMPORT_WORKFLOW && action.getActionStatus() ==
                        ActionStatus.CANCELED) {
                    canceled_workflowJobPids.add(action.getTrackingPid().toString());
                }
                workflowJobPids.add(action.getTrackingPid().toString());
            } else if (isVisibleAction(action)) {
                Job job = new Job();
                job.setName(getActionDisplayName(action));
                job.setJobType(action.getType().getName());
                job.setUser(action.getActionInitiator());
                job.setStartTimestamp(action.getCreated());
                job.setDescription(action.getDescription());
                if (isTimeRangeOnlyDeleteAction(action)) {
                    job.setJobStatus(JobStatus.COMPLETED);
                } else if (!ActionType.getNonWorkflowActions().contains(action.getType())) {
                    if (ActionType.CDL_DATAFEED_IMPORT_WORKFLOW == action.getType()
                            && action.getActionStatus() == ActionStatus.CANCELED) {
                        job.setJobStatus(JobStatus.CANCELLED);
                    } else {
                        job.setJobStatus(JobStatus.RUNNING);
                    }
                } else {
                    job.setJobStatus(JobStatus.COMPLETED);
                }
                jobList.add(job);
            }
        }

        log.debug("workflowJobPids: " + workflowJobPids.toString());
        log.debug("canceled_workflowJobPids: " + canceled_workflowJobPids.toString());
        if (CollectionUtils.isNotEmpty(workflowJobPids)) {
            List<Job> workflowJobs;
            if (MapUtils.isNotEmpty(jobMap)) {
                workflowJobs = new ArrayList<>();
                for (String jobPid : workflowJobPids) {
                    Job job = jobMap.get(jobPid);
                    if (job != null) {
                        workflowJobs.add(job);
                    }
                }
            } else {
                workflowJobs = workflowProxy.getWorkflowExecutionsByJobPids(workflowJobPids,
                        MultiTenantContext.getCustomerSpace().toString());
            }
            if (CollectionUtils.isNotEmpty(canceled_workflowJobPids) && CollectionUtils.isNotEmpty(workflowJobs)) {
                for (Job job : workflowJobs) {
                    if (canceled_workflowJobPids.contains(job.getPid().toString())) {
                        job.setJobStatus(JobStatus.CANCELLED);
                    }
                }
            }
            jobList.addAll(workflowJobs);
        }
        return jobList;
    }

    private boolean isTimeRangeOnlyDeleteAction(Action action) {
        if (action == null || (action.getType() != SOFT_DELETE && action.getType() != HARD_DELETE)) {
            return false;
        }

        try {
            DeleteActionConfiguration config = (DeleteActionConfiguration) action.getActionConfiguration();
            if (config == null) {
                return false;
            }

            return action.getTrackingPid() == null && StringUtils.isNotBlank(config.getFromDate())
                    && StringUtils.isNotBlank(config.getToDate());
        } catch (Exception e) {
            log.warn("Failed to evaluate whether action {} is time range only delete action or not", action.getPid());
            return false;
        }
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

    @VisibleForTesting
    List<Long> getActionPids(List<Job> jobs) {
        List<Long> allActionPids = new ArrayList<>();
        for (Job job : jobs) {
            if (job.getJobType().equals(PA_JOB_TYPE)) {
                List<Long> actionPids = getActionIdsForJob(job);
                if (CollectionUtils.isNotEmpty(actionPids)) {
                    allActionPids.addAll(actionPids);
                }
            }
        }
        return allActionPids;
    }

    @VisibleForTesting
    Map<String, Job> getJobMap(Map<Long, Action> actionIdMap) {
        List<String> workflowJobPids = null;
        if (MapUtils.isNotEmpty(actionIdMap)) {
            workflowJobPids =
                    actionIdMap.values().stream().filter(action -> action.getTrackingPid() != null).map(action -> action.getTrackingPid().toString()).collect(Collectors.toList());
        }
        List<Job> workflowJobs;
        if (CollectionUtils.isNotEmpty(workflowJobPids)) {
            workflowJobs = workflowProxy.getWorkflowExecutionsByJobPids(workflowJobPids,
                    new String[]{MultiTenantContext.getCustomerSpace().toString()});
        } else {
            workflowJobs = new ArrayList<>();
        }
        Map<String, Job> jobMap =
                workflowJobs.stream().filter(job -> job.getPid() != null).collect(Collectors.toMap(job -> job.getPid().toString(),
                        Job -> Job, (key1, key2) -> key2));
        return jobMap;
    }

    @VisibleForTesting
    void updateAllJobs(List<Job> jobs) {
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

        updateExpiredOrphanJobs(jobs);
        updateJobsWithActionId(jobs, null);

        List<Long> allActionPids = getActionPids(jobs);
        Map<Long, Action> actionIdMap = new HashMap<>();
        Map<Long, Action> actionJobPidMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(allActionPids)) {
            List<Action> actions = getActions(allActionPids, null);
            actions.forEach(action -> {
                if (action.getPid() != null) {
                    actionIdMap.put(action.getPid(), action);
                }
                if (action.getTrackingPid() != null) {
                    actionJobPidMap.put(action.getTrackingPid(), action);
                }
            });
        }
        Map<String, Job> jobMap = getJobMap(actionIdMap);

        for (Job job : jobs) {
            updateStepDisplayNameAndNumSteps(job);
            updateJobDisplayNameAndDescription(job);
            updateJobWithModelSummaryInfo(job, true, modelIdToModelSummaries);
            if (hasCG) {
                updateJobWithRatingEngineSummaryInfo(job, true, ratingIdToRatingEngineSummaries);
            }
            updateJobWithSubJobsIfIsPnA(job, jobMap, actionIdMap, actionJobPidMap);
        }
    }

    @VisibleForTesting
    void updateExpiredOrphanJobs(List<Job> jobs) {
        Optional<Job> latestPAJob = jobs.stream()
                .filter(job -> job.getJobType().equals(PA_JOB_TYPE))
                .min((job1, job2) -> job2.getStartTimestamp().compareTo(job1.getStartTimestamp()));
        Optional<Job> latestOrphanTrxJob = jobs.stream()
                .filter(job -> job.getJobType().equals(ORPHAN_JOB_TYPE) && validateArtifactTypeAndOrphanType(
                        job.getInputs().get("ARTIFACT_TYPE"), OrphanRecordsType.TRANSACTION))
                .min((job1, job2) -> job2.getStartTimestamp().compareTo(job1.getStartTimestamp()));
        Optional<Job> latestOrphanContactJob = jobs.stream()
                .filter(job -> job.getJobType().equals(ORPHAN_JOB_TYPE) && validateArtifactTypeAndOrphanType(
                        job.getInputs().get("ARTIFACT_TYPE"), OrphanRecordsType.CONTACT))
                .min((job1, job2) -> job2.getStartTimestamp().compareTo(job1.getStartTimestamp()));
        Optional<Job> latestUnmatchedAccountJob = jobs.stream()
                .filter(job -> job.getJobType().equals(ORPHAN_JOB_TYPE) && validateArtifactTypeAndOrphanType(
                        job.getInputs().get("ARTIFACT_TYPE"), OrphanRecordsType.UNMATCHED_ACCOUNT))
                .min((job1, job2) -> job2.getStartTimestamp().compareTo(job1.getStartTimestamp()));

        for (Job job : jobs) {
            if (job.getJobType().equals(ORPHAN_JOB_TYPE)) {
                // check by PA job
                if (latestPAJob.isPresent()
                        && latestPAJob.get().getStartTimestamp().compareTo(job.getStartTimestamp()) > 0) {
                    job.getInputs().put("EXPORT_ID", ORPHAN_ARTIFACT_EXPIRED);
                }

                // check by the same orphan types
                if (latestOrphanTrxJob.isPresent()
                        && validateArtifactTypeAndOrphanType(
                        job.getInputs().get("ARTIFACT_TYPE"), OrphanRecordsType.TRANSACTION)
                        && latestOrphanTrxJob.get().getStartTimestamp().compareTo(job.getStartTimestamp()) > 0) {
                    job.getInputs().put("EXPORT_ID", ORPHAN_ARTIFACT_EXPIRED);
                }

                if (latestOrphanContactJob.isPresent()
                        && validateArtifactTypeAndOrphanType(
                        job.getInputs().get("ARTIFACT_TYPE"), OrphanRecordsType.CONTACT)
                        && latestOrphanContactJob.get().getStartTimestamp().compareTo(job.getStartTimestamp()) > 0) {
                    job.getInputs().put("EXPORT_ID", ORPHAN_ARTIFACT_EXPIRED);
                }

                if (latestUnmatchedAccountJob.isPresent()
                        && validateArtifactTypeAndOrphanType(
                        job.getInputs().get("ARTIFACT_TYPE"), OrphanRecordsType.UNMATCHED_ACCOUNT)
                        && latestUnmatchedAccountJob.get().getStartTimestamp().compareTo(job.getStartTimestamp()) > 0) {
                    job.getInputs().put("EXPORT_ID", ORPHAN_ARTIFACT_EXPIRED);
                }
            }
        }
    }

    @VisibleForTesting
    void updateJobWithSubJobsIfIsPnA(Job job, Map<String, Job> jobMap, Map<Long, Action> actionMap,
                                     Map<Long, Action> actionJobPidMap) {
        if (job.getJobType().equals(PA_JOB_TYPE)) {
            List<Long> actionPids = getActionIdsForJob(job);
            if (CollectionUtils.isNotEmpty(actionPids)) {
                List<Action> actions = getActions(actionPids, actionMap);
                List<Job> subJobs = expandActions(actions, jobMap);
                job.setSubJobs(updateJobsWithActionId(subJobs, actionJobPidMap));
            }
        }
    }

    @VisibleForTesting
    List<Job> updateJobsWithActionId(List<Job> jobs, Map<Long, Action> actionJobPidMap) {
        Set<Long> jobPids = new HashSet<>();
        Map<Long, Job> updateJobs = new HashMap<>();
        for (Job job : jobs) {
            if (PA_JOB_TYPE.equals(job.getJobType())) {
                continue;
            }
            if (job.getInputs() == null) {
                if (job.getPid() != null) {
                    jobPids.add(job.getPid());
                    updateJobs.put(job.getPid(), job);
                }
            } else {
                String actionIdsStr = job.getInputs().get(WorkflowContextConstants.Inputs.ACTION_ID);
                if (StringUtils.isEmpty(actionIdsStr) && job.getPid() != null) {
                    jobPids.add(job.getPid());
                    updateJobs.put(job.getPid(), job);
                }
            }
        }
        jobs.removeAll(updateJobs.values());
        if (CollectionUtils.isNotEmpty(jobPids)) {
            String tenantId = MultiTenantContext.getShortTenantId();
            List<Action> actions;
            if (MapUtils.isNotEmpty(actionJobPidMap)) {
                actions = new ArrayList<>();
                for (Long jobPid : jobPids) {
                    Action action = actionJobPidMap.get(jobPid);
                    if (action != null) {
                        actions.add(action);
                    }
                }
            } else {
                actions = actionProxy.getActionsByJobPids(tenantId, new ArrayList<>(jobPids));
            }
            if (CollectionUtils.isNotEmpty(actions)) {
                for (Action action : actions) {
                    Job actionJob = updateJobs.get(action.getTrackingPid());
                    if (actionJob != null) {
                        if (actionJob.getInputs() == null) {
                            Map<String, String> inputContext = new HashMap<>();
                            actionJob.setInputs(inputContext);
                        }
                        String actionIdsStr = actionJob.getInputs().get(WorkflowContextConstants.Inputs.ACTION_ID);
                        if (StringUtils.isNotEmpty(actionIdsStr)) {
                            log.warn("this trackingPid " + action.getTrackingPid() + " has multi action, using the first one");
                        }
                        actionJob.getInputs().put(WorkflowContextConstants.Inputs.ACTION_ID,
                                String.valueOf(action.getPid()));
                        updateJobs.put(action.getTrackingPid(), actionJob);
                    }
                }
            }
        }
        jobs.addAll(updateJobs.values());
        return jobs;
    }

    private boolean validateArtifactTypeAndOrphanType(String artifactType, OrphanRecordsType orphanRecordsType) {
        return artifactType.equals(orphanRecordsType.name()) || artifactType.equals(orphanRecordsType.getOrphanType());
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

        ModelSummary modelSummary;
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
        return ApplicationIdUtils.toApplicationIdObj(applicationId);
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

    private String getActionDisplayName(Action action) {
        String displayName = action.getType().getDisplayName();
        if (!action.getType().equals(ActionType.DATA_REPLACE) || (action.getType().equals(ActionType.DATA_REPLACE) && !(action.getActionConfiguration() instanceof CleanupActionConfiguration))) {
            return displayName;
        }
        CleanupActionConfiguration cleanupActionConfiguration = (CleanupActionConfiguration) action.getActionConfiguration();
        if (CollectionUtils.isEmpty(cleanupActionConfiguration.getImpactEntities())) {
            return displayName;
        }
        displayName = String.format("%s: %s", displayName, cleanupActionConfiguration.getImpactEntities().get(0));
        return displayName;
    }
}
