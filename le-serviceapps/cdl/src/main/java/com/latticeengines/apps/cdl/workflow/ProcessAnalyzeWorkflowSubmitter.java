package com.latticeengines.apps.cdl.workflow;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.ImportMigrateTrackingService;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.apps.core.util.FeatureFlagUtils;
import com.latticeengines.apps.core.util.UpdateTransformDefinitionsUtils;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.metadata.entitymgr.MigrationTrackEntityMgr;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class ProcessAnalyzeWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeWorkflowSubmitter.class);

    @Inject
    private MigrationTrackEntityMgr migrationTrackEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private ImportMigrateTrackingService importMigrateTrackingService;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${cdl.transform.workflow.mem.mb}")
    private int workflowMemMb;

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @Value("${cdl.processAnalyze.actions.import.count}")
    private int importActionCount;

    @Value("${cdl.pa.default.max.iteration}")
    private int defaultMaxIteration;

    @Resource(name = "jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    private final DataCollectionService dataCollectionService;

    private final DataFeedService dataFeedService;

    private final DataFeedTaskService dataFeedTaskService;

    private final WorkflowProxy workflowProxy;

    private final ColumnMetadataProxy columnMetadataProxy;

    private final ActionService actionService;

    private final BatonService batonService;

    private final ZKConfigService zkConfigService;

    private final CDLAttrConfigProxy cdlAttrConfigProxy;

    @Inject
    public ProcessAnalyzeWorkflowSubmitter(DataCollectionService dataCollectionService,
                                           DataFeedService dataFeedService, DataFeedTaskService dataFeedTaskService,  //
                                           WorkflowProxy workflowProxy, ColumnMetadataProxy columnMetadataProxy, ActionService actionService,
                                           BatonService batonService, ZKConfigService zkConfigService, CDLAttrConfigProxy cdlAttrConfigProxy) {
        this.dataCollectionService = dataCollectionService;
        this.dataFeedService = dataFeedService;
        this.dataFeedTaskService = dataFeedTaskService;
        this.workflowProxy = workflowProxy;
        this.columnMetadataProxy = columnMetadataProxy;
        this.actionService = actionService;
        this.batonService = batonService;
        this.zkConfigService = zkConfigService;
        this.cdlAttrConfigProxy = cdlAttrConfigProxy;
    }

    @WithWorkflowJobPid
    public ApplicationId submit(String customerSpace, ProcessAnalyzeRequest request, WorkflowPidWrapper pidWrapper) {
        log.info(String.format("WorkflowJob created for customer=%s with pid=%s", customerSpace, pidWrapper.getPid()));
        if (customerSpace == null) {
            throw new IllegalArgumentException("There is not CustomerSpace in MultiTenantContext");
        }
        boolean tenantInMigration = migrationTrackEntityMgr.tenantInMigration(tenantEntityMgr.findByTenantId(customerSpace));
        if (!request.skipMigrationCheck && tenantInMigration) {
            log.error("Tenant {} is in migration and should not kickoff PA.", customerSpace);
            throw new IllegalStateException(String.format("Tenant %s is in migration.", customerSpace));
        } else if (request.skipMigrationCheck && !tenantInMigration) {
            log.error("Tenant {} is not in migration and should not kickoff migration PA", customerSpace);
            throw new IllegalStateException(String.format("Tenant %s is not in migration.", customerSpace));
        }
        DataCollection dataCollection = dataCollectionService.getDataCollection(customerSpace, null);
        if (dataCollection == null) {
            throw new LedpException(LedpCode.LEDP_37014);
        }

        DataFeed datafeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        Status datafeedStatus = datafeed.getStatus();
        log.info(String.format("customer: %s, data feed: %s, status: %s", customerSpace, datafeed.getName(),
                datafeedStatus.getName()));

        AttrConfigRequest configRequest = cdlAttrConfigProxy.validateAttrConfig(customerSpace, new AttrConfigRequest(),
                AttrConfigUpdateMode.Limit);
        if (configRequest.hasError()) {
            throw new RuntimeException("User activate or enable more allowed attribute.");
        }

        try {
            List<Map<String, Object>> props = jdbcTemplate.queryForList("show variables like '%wait_timeout%'");
            log.info("Timeout Configuration from DB Session: " + props);
        } catch (Exception e) {
            log.warn("Failed to check timeout configuration from DB session.", e);
        }
        if (tenantInMigration) {
            try {
                return submitWithOnlyImportActions(customerSpace, request, datafeed, pidWrapper);
            } catch (Exception e) {
                log.error(String.format("Failed to submit %s's P&A workflow", customerSpace)
                        + ExceptionUtils.getStackTrace(e));
                dataFeedService.failExecution(customerSpace, "", datafeedStatus.getName());
                throw new RuntimeException(String.format("Failed to submit %s's P&A migration workflow", customerSpace), e);
            }
        }
        List<Action> lastFailedActions = getActionsFromLastFailedPA(customerSpace,
                request.isInheritAllCompleteImportActions(), request.getImportActionPidsToInherit());

        if (dataFeedService.lockExecution(customerSpace, "", DataFeedExecutionJobType.PA) == null) {
            String errorMessage;
            if (Status.Initing.equals(datafeedStatus) || Status.Initialized.equals(datafeedStatus)) {
                errorMessage = String.format(
                        "We can't start processAnalyze workflow for %s, need to import data first.", customerSpace);
            } else {
                errorMessage = String.format("We can't start processAnalyze workflow for %s by dataFeedStatus %s",
                        customerSpace, datafeedStatus.getName());
            }

            throw new RuntimeException(errorMessage);
        }

        Status initialStatus = getInitialDataFeedStatus(datafeedStatus);

        log.info(String.format("customer %s data feed %s initial status: %s", customerSpace, datafeed.getName(),
                initialStatus.getName()));

        log.info(String.format("Submitting process and analyze workflow for customer %s", customerSpace));

        try {
            Set<BusinessEntity> needDeletedEntity = new HashSet<>();
            List<Long> actionIds = getActionIds(customerSpace, needDeletedEntity);
            if (CollectionUtils.isNotEmpty(lastFailedActions)) {
                List<Long> lastFailedActionIds = lastFailedActions.stream().map(Action::getPid)
                        .collect(Collectors.toList());
                log.info(
                        "Inherit actions from last failed processAnalyze workflow, inheritAllImportActions={}, "
                                + "importActionPIds={}, inherited actionPids={}",
                        request.isInheritAllCompleteImportActions(), request.getImportActionPidsToInherit(),
                        lastFailedActionIds);
                // create copies of last failed actions
                lastFailedActions.forEach(action -> {
                    action.setPid(null);
                    action.setOwnerId(pidWrapper.getPid());
                });
                lastFailedActions = actionService.copy(lastFailedActions);
                List<Long> copiedActionIds = lastFailedActions.stream().map(Action::getPid)
                        .collect(Collectors.toList());
                // add to front
                actionIds.addAll(0, copiedActionIds);
            }
            updateActions(actionIds, pidWrapper.getPid());
            List<Long> canceledActionPids = getCanceledActionIds(customerSpace);
            updateActions(canceledActionPids, pidWrapper.getPid());

            String currentDataCloudBuildNumber = columnMetadataProxy.latestBuildNumber();
            ProcessAnalyzeWorkflowConfiguration configuration = generateConfiguration(customerSpace, request,
                    actionIds, needDeletedEntity,
                    initialStatus, currentDataCloudBuildNumber, pidWrapper.getPid());

            configuration.setFailingStep(request.getFailingStep());

            return workflowJobService.submit(configuration, pidWrapper.getPid());
        } catch (Exception e) {
            log.error(String.format("Failed to submit %s's P&A workflow", customerSpace)
                    + ExceptionUtils.getStackTrace(e));
            dataFeedService.failExecution(customerSpace, "", datafeedStatus.getName());
            throw new RuntimeException(String.format("Failed to submit %s's P&A workflow", customerSpace), e);
        }
    }

    private Status getInitialDataFeedStatus(Status status) {
        if (status.equals(Status.ProcessAnalyzing) || status.equals(Status.Deleting)) {
            return Status.Active;
        } else {
            return status;
        }
    }

    @VisibleForTesting
    List<Long> getActionIds(String customerSpace, Set<BusinessEntity> needDeletedEntity) {
        List<Action> actions = actionService.findByOwnerId(null);
        log.info(String.format("Actions are %s for tenant=%s", Arrays.toString(actions.toArray()), customerSpace));
        Set<ActionType> importAndDeleteTypes = Sets.newHashSet( //
                ActionType.CDL_DATAFEED_IMPORT_WORKFLOW, //
                ActionType.CDL_OPERATION_WORKFLOW);
        // TODO add status filter to filter out running ones
        List<String> importAndDeleteJobPidStrs = actions.stream()
                .filter(action -> importAndDeleteTypes.contains(action.getType()) && action.getTrackingPid() != null
                        && action.getActionStatus() != ActionStatus.CANCELED)
                .map(action -> action.getTrackingPid().toString()).collect(Collectors.toList());
        log.info(String.format("importAndDeleteJobPidStrs are %s", importAndDeleteJobPidStrs));
        List<Job> importAndDeleteJobs = workflowProxy.getWorkflowExecutionsByJobPids(importAndDeleteJobPidStrs,
                customerSpace);

        List<Long> completedImportAndDeleteJobPids = CollectionUtils.isEmpty(importAndDeleteJobs)
                ? Collections.emptyList()
                : importAndDeleteJobs.stream().filter(
                job -> job.getJobStatus() != JobStatus.PENDING && job.getJobStatus() != JobStatus.RUNNING)
                .map(Job::getPid).collect(Collectors.toList());
        log.info(String.format("Job pids that associated with the current consolidate job are: %s",
                completedImportAndDeleteJobPids));

        Set<Long> importActionIds = new HashSet<>();
        List<Action> completedActions = actions.stream()
                .filter(action -> isCompleteAction(action, completedImportAndDeleteJobPids, importActionIds)).collect(Collectors.toList());
        List<Action> nonWorkflowDeleteActions =
                actions.stream().filter(action -> action.getTrackingPid() == null && action.getType() == ActionType.CDL_OPERATION_WORKFLOW).collect(Collectors.toList());
        Set<BusinessEntity> importedEntities = getImportEntities(customerSpace, completedActions);
        //in old logic, delete operation done by cdlOperationWorkflow, so we don't need pick it at pa to delete again
        // in new logic, delete operation done by pa step, so we need pick the entity we need deleted,
        // waiting compare with import entity to judge if we can delete or not
        Set<BusinessEntity> totalNeedDeletedEntities = getDeletedEntities(nonWorkflowDeleteActions);
        needDeletedEntity.addAll(getCurrentDeletedEntities(importedEntities, totalNeedDeletedEntities,
                importActionIds.size()));
        List<Long> completedActionIds = completedActions.stream().map(Action::getPid).collect(Collectors.toList());
        completedActionIds.addAll(nonWorkflowDeleteActions.stream().map(Action::getPid).collect(Collectors.toList()));
        log.info(String.format("Actions that associated with the current consolidate job are: %s", completedActionIds));

        return completedActionIds;
    }

    @VisibleForTesting
    List<Long> getCanceledActionIds(String customerSpace) {
        List<Action> actions = actionService.findByOwnerId(null);
        log.info(String.format("Actions are %s for tenant=%s", Arrays.toString(actions.toArray()), customerSpace));
        List<Long> canceledActionPids = actions.stream()
                .filter(action -> action.getType() == ActionType.CDL_DATAFEED_IMPORT_WORKFLOW
                        && action.getOwnerId() == null && action.getActionStatus() == ActionStatus.CANCELED)
                .map(Action::getPid).collect(Collectors.toList());
        return canceledActionPids;
    }

    /*
     * Retrieve all the inheritable action IDs if the last PA failed. Return an empty list otherwise.
     */
    @VisibleForTesting
    List<Action> getActionsFromLastFailedPA(@NotNull String customerSpace, boolean inheritAllCompleteImportActions,
                                            List<Long> importActionPidsToInherit) {
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace);
        DataFeedExecution lastDataFeedExecution = dataFeedService.getLatestExecution(customerSpace, dataFeed.getName(),
                DataFeedExecutionJobType.PA);
        if (lastDataFeedExecution == null || lastDataFeedExecution.getWorkflowId() == null) {
            return Collections.emptyList();
        }

        // data feed execution does not always have workflowPid, need the
        // workflow execution
        Long workflowId = lastDataFeedExecution.getWorkflowId();
        Job job = workflowProxy.getWorkflowExecution(String.valueOf(workflowId), true);
        if (job == null || job.getPid() == null || job.getJobStatus() != JobStatus.FAILED) {
            return Collections.emptyList();
        }
        Long workflowPid = job.getPid();
        checkImportActionIds(workflowPid, importActionPidsToInherit);

        List<Action> actions = actionService.findByOwnerId(workflowPid).stream().filter(action -> {
            if (action == null || action.getPid() == null || action.getType() == null) {
                return false;
            }

            // import actions
            if (ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())) {
                if (CollectionUtils.isNotEmpty(importActionPidsToInherit)
                        && importActionPidsToInherit.contains(action.getPid())) {
                    // user gives a list of action PIDs to inherit and the action is in that list
                    return true;
                } else if (inheritAllCompleteImportActions) {
                    // only consider the flag if there is no input list of action PIDs given
                    return true;
                }
            }

            // not inherit system actions and import actions by default (unless flag/list is
            // given)
            return !ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())
                    && !ActionType.getDataCloudRelatedTypes().contains(action.getType());
        }).collect(Collectors.toList());
        Set<Long> completedPIds = getCompletedImportActionPids(actions);
        log.info("PID of last failed PA = {}, all completed import action PIDs = {}", workflowPid, completedPIds);
        return actions.stream().filter(action -> {
            if (!ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())) {
                // all non-import actions already inheritable at this point
                return true;
            }

            return completedPIds.contains(action.getPid());
        }).collect(Collectors.toList());
    }

    @VisibleForTesting
    Set<Long> getCompletedImportActionPids(@NotNull List<Action> actions) {
        if (CollectionUtils.isEmpty(actions)) {
            return Collections.emptySet();
        }

        List<Long> importWorkflowPIds = actions.stream() //
                .filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())) //
                .map(Action::getTrackingPid) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toList());
        if (importWorkflowPIds.isEmpty()) {
            return Collections.emptySet();
        }
        List<String> pidStrs = importWorkflowPIds.stream().map(String::valueOf).collect(Collectors.toList());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsByJobPids(pidStrs);
        if (CollectionUtils.isEmpty(jobs)) {
            return Collections.emptySet();
        }

        Set<Long> completedJobPIds = jobs.stream() //
                .filter(job -> JobStatus.COMPLETED.equals(job.getJobStatus())) //
                .map(Job::getPid) //
                .collect(Collectors.toSet());
        return actions.stream() //
                .filter(action -> action.getTrackingPid() != null) //
                .filter(action -> completedJobPIds.contains(action.getTrackingPid())) //
                .map(Action::getPid) //
                .collect(Collectors.toSet());
    }

    /*
     * Make sure input action PIDs all belong to import actions owned by last failed PA
     */
    @VisibleForTesting
    void checkImportActionIds(long workflowPid, List<Long> importActionPIds) {
        if (CollectionUtils.isEmpty(importActionPIds)) {
            return;
        }
        // make sure input PIDs are valid
        importActionPIds.forEach(Preconditions::checkNotNull);

        // find all actions in the list that are (a) owned by the workflow and (b) is
        // import workflow
        Map<Long, Action> importActions = actionService.findByPidIn(importActionPIds) //
                .stream() //
                .filter(action -> action.getOwnerId() != null && action.getOwnerId().equals(workflowPid)) //
                .filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())) //
                .collect(Collectors.toMap(Action::getPid, action -> action, (v1, v2) -> v1));
        // get all invalid action PIDs from the input list
        List<Long> invalidActionPIds = importActionPIds //
                .stream() //
                .filter(pid -> !importActions.containsKey(pid)) //
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(invalidActionPIds)) {
            String msg = String.format(
                    "Following actions are either not owned by the last failed PA or not import actions: %s",
                    invalidActionPIds);
            throw new IllegalArgumentException(msg);
        }
    }

    private void updateActions(List<Long> actionIds, Long workflowPid) {
        log.info(String.format("Updating actions=%s with ownerId=%d", Arrays.toString(actionIds.toArray()),
                workflowPid));
        if (CollectionUtils.isNotEmpty(actionIds)) {
            actionService.patchOwnerIdByPids(workflowPid, actionIds);
        }
    }

    private Set<BusinessEntity> getCurrentDeletedEntities(Set<BusinessEntity> importedEntity,
                                                          Set<BusinessEntity> needDeletedEntity,
                                                          int importActionNum) {
        //judge this entity which need delete entity has import action, if not, check if importActionNum is over than
        // limit, if not, cannot deleted, else delete those entity that we can find it in current importAction.
        if (needDeletedEntity.size() != importedEntity.size() && needDeletedEntity.size() != 0 && importActionNum < importActionCount) {
            throw new RuntimeException(String.format("deleteEntity %s isn't match importEntity %s",
                    JsonUtils.serialize(needDeletedEntity), JsonUtils.serialize(importedEntity)));
        } else {
            Set<BusinessEntity> currentNeedDeletedEntity = new HashSet<>(needDeletedEntity);
            currentNeedDeletedEntity.retainAll(importedEntity);
            return currentNeedDeletedEntity;
        }
    }

    private boolean isCompleteAction(Action action, List<Long> completedImportAndDeleteJobPids,
                                     Set<Long> importActionIds) {
        boolean isComplete = true; // by default every action is valid
        if (ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())) {
            // special check if is selected type
            isComplete = false;
            if (importActionCount > 0 && importActionIds.size() >= importActionCount) {
                return false;
            }
            if (completedImportAndDeleteJobPids.contains(action.getTrackingPid())) {
                importActionIds.add(action.getPid());
                isComplete = true;
            } else {
                ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action
                        .getActionConfiguration();
                if (importActionConfiguration == null) {
                    log.error("Import action configuration is null!");
                    return false;
                }
                if (Boolean.TRUE.equals(importActionConfiguration.getMockCompleted())) {
                    importActionIds.add(action.getPid());
                    isComplete = true;
                }
            }
        } else if (ActionType.CDL_OPERATION_WORKFLOW.equals(action.getType())) {
            isComplete = false;
            if (completedImportAndDeleteJobPids.contains(action.getTrackingPid())) {
                isComplete = true;
            }
        }
        return isComplete;
    }

    private ProcessAnalyzeWorkflowConfiguration generateConfiguration(String customerSpace,
                                                                      ProcessAnalyzeRequest request,
                                                                      List<Long> actionIds,
                                                                      Set<BusinessEntity> needDeletedEntities,
                                                                      Status status,
                                                                      String currentDataCloudBuildNumber,
                                                                      long workflowPid) {
        DataCloudVersion dataCloudVersion = columnMetadataProxy.latestVersion(null);
        String scoringQueue = LedpQueueAssigner.getScoringQueueNameForSubmission();

        FeatureFlagValueMap flags = batonService.getFeatureFlags(MultiTenantContext.getCustomerSpace());
        TransformationGroup transformationGroup = FeatureFlagUtils.getTransformationGroupFromZK(flags);
        boolean entityMatchEnabled = FeatureFlagUtils.isEntityMatchEnabled(flags);
        boolean targetScoreDerivationEnabled = FeatureFlagUtils.isTargetScoreDerivation(flags);
        boolean alwaysOnCampain = FeatureFlagUtils.isAlwaysOnCampaign(flags);
        log.info("Submitting PA: FeatureFlags={}, tenant={}, entityMatchEnabled={}, workflowPid={}", flags,
                customerSpace, entityMatchEnabled, workflowPid);
        if (entityMatchEnabled && Boolean.TRUE.equals(request.getFullRematch())) {
            throw new UnsupportedOperationException("Full rematch is not supported for entity match tenants yet.");
        }
        boolean apsImputationEnabled = FeatureFlagUtils.isApsImputationEnabled(flags);
        List<TransformDefinition> stdTransformDefns = UpdateTransformDefinitionsUtils
                .getTransformDefinitions(SchemaInterpretation.SalesforceAccount.toString(), transformationGroup);

        boolean entityMatchGAOnly = FeatureFlagUtils.isEntityMatchGAOnly(flags);
        boolean internalEnrichEnabled = zkConfigService.isInternalEnrichmentEnabled(CustomerSpace.parse(customerSpace));
        int maxIteration = request.getMaxRatingIterations() != null ? request.getMaxRatingIterations()
                : defaultMaxIteration;
        String apsRollingPeriod = zkConfigService
                .getRollingPeriod(CustomerSpace.parse(customerSpace), CDLComponent.componentName).getPeriodName();
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS, status.getName());
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "processAnalyzeWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.DATAFEED_STATUS, status.getName());
        inputProperties.put(WorkflowContextConstants.Inputs.ALWAYS_ON_CAMPAIGNS, String.valueOf(alwaysOnCampain));
        inputProperties.put(WorkflowContextConstants.Inputs.ACTION_IDS, JsonUtils.serialize(actionIds));
        return new ProcessAnalyzeWorkflowConfiguration.Builder() //
                .microServiceHostPort(microserviceHostPort) //
                .customer(CustomerSpace.parse(customerSpace)) //
                .internalResourceHostPort(internalResourceHostPort) //
                .initialDataFeedStatus(status) //
                .actionIds(actionIds) //
                .ownerId(workflowPid) //
                .deletedEntities(needDeletedEntities) //
                .rebuildEntities(request.getRebuildEntities()) //
                .rebuildSteps(request.getRebuildSteps()) //
                .ignoreDataCloudChange(request.getIgnoreDataCloudChange()) //
                .userId(request.getUserId()) //
                .dataCloudVersion(dataCloudVersion) //
                .allowInternalEnrichAttrs(internalEnrichEnabled) //
                .matchYarnQueue(scoringQueue) //
                .inputProperties(inputProperties) //
                .workflowContainerMem(workflowMemMb) //
                .currentDataCloudBuildNumber(currentDataCloudBuildNumber) //
                .transformationGroup(transformationGroup, stdTransformDefns) //
                .dynamoSignature(signature) //
                .maxRatingIteration(maxIteration) //
                .apsRollingPeriod(apsRollingPeriod) //
                .apsImputationEnabled(apsImputationEnabled) //
                .entityMatchEnabled(entityMatchEnabled) //
                .entityMatchGAOnly(entityMatchGAOnly) //
                .targetScoreDerivationEnabled(targetScoreDerivationEnabled) //
                .fullRematch(Boolean.TRUE.equals(request.getFullRematch())) //
                .autoSchedule(Boolean.TRUE.equals(request.getAutoSchedule())) //
                .skipEntities(request.getSkipEntities()) //
                .skipPublishToS3(Boolean.TRUE.equals(request.getSkipPublishToS3())) //
                .skipDynamoExport(Boolean.TRUE.equals(request.getSkipDynamoExport())) //
                .build();
    }

    public ApplicationId retryLatestFailed(String customerSpace, Integer memory, Boolean autoRetry, Boolean skipMigrationCheck) {
        if (!skipMigrationCheck && migrationTrackEntityMgr.tenantInMigration(tenantEntityMgr.findByTenantId(customerSpace))) {
            log.error("Tenant {} is in migration and should not kickoff PA.", customerSpace);
            throw new IllegalStateException(String.format("Tenant %s is in migration.", customerSpace));
        }
        DataFeed datafeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        List<Action> lastFailedActions = getActionsFromLastFailedPA(customerSpace, true, null);
        Long workflowId = dataFeedService.restartExecution(customerSpace, "", DataFeedExecutionJobType.PA);
        checkWorkflowId(customerSpace, datafeed, workflowId);

        try {
            log.info(String.format("restarted execution with workflowId=%s", workflowId));
            ApplicationId appId = workflowJobService.restart(workflowId, customerSpace, memory, autoRetry);
            if (appId != null && CollectionUtils.isNotEmpty(lastFailedActions)) {
                Job retryJob = workflowProxy.getWorkflowJobFromApplicationId(appId.toString());
                // get IDs before we clear them
                List<Long> lastFailedActionIds = lastFailedActions.stream().map(Action::getPid)
                        .collect(Collectors.toList());
                // clear PID to create new action that has the same content as
                // the old ones
                // NOTE that since we don't update retry job's input context,
                // action IDs in context
                // will not be the same as the ones owned by the retry job
                lastFailedActions.forEach(action -> {
                    action.setPid(null);
                    action.setOwnerId(retryJob.getPid());
                });
                actionService.copy(lastFailedActions);

                log.info("Inherit actions from last failed processAnalyze workflow in retry, actionIds={}",
                        lastFailedActionIds);
            }
            return appId;
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            dataFeedService.failExecution(customerSpace, "", datafeed.getStatus().getName());
            throw new RuntimeException(String.format("Failed to retry %s's P&A workflow", customerSpace));
        }
    }

    private void checkWorkflowId(String customerSpace, DataFeed datafeed, Long workflowId) {
        if (workflowId == null) {
            dataFeedService.failExecution(customerSpace, "", datafeed.getStatus().getName());
            String msg = String.format(
                    "Failed to retry %s's P&A workflow because there's no workflow Id, run the new P&A workflow instead.",
                    customerSpace);
            log.warn(msg);
            throw new RuntimeException(msg);
        }
    }

    private ApplicationId submitWithOnlyImportActions(String customerSpace, ProcessAnalyzeRequest request, DataFeed datafeed, WorkflowPidWrapper pidWrapper) {
        checkImportTrackingLinked(customerSpace);
        List<Long> importActionIds = getImportActionIds(customerSpace);
        Status datafeedStatus = datafeed.getStatus();

        if (dataFeedService.lockExecution(customerSpace, "", DataFeedExecutionJobType.PA) == null) {
            String errorMessage;
            if (Status.Initing.equals(datafeedStatus) || Status.Initialized.equals(datafeedStatus)) {
                errorMessage = String.format(
                        "We can't start processAnalyze workflow for %s, need to import data first.", customerSpace);
            } else {
                errorMessage = String.format("We can't start processAnalyze workflow for %s by dataFeedStatus %s",
                        customerSpace, datafeedStatus.getName());
            }

            throw new RuntimeException(errorMessage);
        }

        Status initialStatus = getInitialDataFeedStatus(datafeedStatus);

        log.info("customer {} data feed {} initial status: {}", customerSpace, datafeed.getName(),
                initialStatus.getName());

        log.info("Submitting migration PA workflow for customer {}", customerSpace);

        updateActions(importActionIds, pidWrapper.getPid());

        String currentDataCloudBuildNumber = columnMetadataProxy.latestBuildNumber();
        ProcessAnalyzeWorkflowConfiguration configuration = generateConfiguration(customerSpace, request,
                importActionIds, new HashSet<>(),
                initialStatus, currentDataCloudBuildNumber, pidWrapper.getPid());

        configuration.setFailingStep(request.getFailingStep());

        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private void checkImportTrackingLinked(String customerSpace) {
        if (migrationTrackEntityMgr.findByTenant(tenantEntityMgr.findByTenantId(customerSpace)).getImportMigrateTracking() == null) {
            throw new IllegalStateException("No import migrate tracking record linked to current migration tracking record.");
        }
    }

    private List<Long> getImportActionIds(String customerSpace) {
        Long importMigrateTrackingPid = migrationTrackEntityMgr.findByTenant(tenantEntityMgr.findByTenantId(customerSpace)).getImportMigrateTracking().getPid();
        return importMigrateTrackingService.getAllRegisteredActionIds(customerSpace, importMigrateTrackingPid);
    }

    private Set<BusinessEntity> getImportEntities(String customerSpace, List<Action> actions) {
        Set<BusinessEntity> importedEntity = new HashSet<>();
        if (CollectionUtils.isNotEmpty(actions)) {
            for (Action action : actions) {
                if (ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())) {
                    if (importedEntity.size() >= 4) {
                        break;
                    }
                    //if entity list isn't completed, do this logic to check import entity
                    if (action.getActionConfiguration() instanceof ImportActionConfiguration) {
                        ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action.getActionConfiguration();
                        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace,
                                importActionConfiguration.getDataFeedTaskId());
                        importedEntity.add(BusinessEntity.getByName(dataFeedTask.getEntity()));
                    }
                }
            }
        }
        return importedEntity;
    }

    private Set<BusinessEntity> getDeletedEntities(List<Action> actions) {
        Set<BusinessEntity> needDeletedEntity = new HashSet<>();
        if (CollectionUtils.isNotEmpty(actions)) {
            for (Action action : actions) {
                if (needDeletedEntity.size() >= 4) {
                    break;
                }
                if (ActionType.CDL_OPERATION_WORKFLOW.equals(action.getType())) {
                    if (action.getActionConfiguration() instanceof CleanupActionConfiguration && needDeletedEntity.size() < 4) {
                        CleanupActionConfiguration cleanupActionConfiguration = (CleanupActionConfiguration) action.getActionConfiguration();
                        needDeletedEntity.addAll(cleanupActionConfiguration.getImpactEntities());
                    }
                }
            }
        }
        return needDeletedEntity;
    }
}
