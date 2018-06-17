package com.latticeengines.apps.cdl.workflow;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.latticeengines.apps.cdl.service.ZKConfigService;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.util.FeatureFlagUtils;
import com.latticeengines.apps.core.util.UpdateTransformDefinitionsUtils;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
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
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class ProcessAnalyzeWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeWorkflowSubmitter.class);

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${cdl.transform.workflow.mem.mb}")
    protected int workflowMemMb;

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    private final DataCollectionProxy dataCollectionProxy;

    private final DataFeedProxy dataFeedProxy;

    private final WorkflowProxy workflowProxy;

    private final ColumnMetadataProxy columnMetadataProxy;

    private final ActionService actionService;

    private final BatonService batonService;

    private final ZKConfigService zkConfigService;

    @Inject
    public ProcessAnalyzeWorkflowSubmitter(DataCollectionProxy dataCollectionProxy, DataFeedProxy dataFeedProxy, //
            WorkflowProxy workflowProxy, ColumnMetadataProxy columnMetadataProxy, ActionService actionService,
            BatonService batonService, ZKConfigService zkConfigService) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.dataFeedProxy = dataFeedProxy;
        this.workflowProxy = workflowProxy;
        this.columnMetadataProxy = columnMetadataProxy;
        this.actionService = actionService;
        this.batonService = batonService;
        this.zkConfigService = zkConfigService;
    }

    @WithWorkflowJobPid
    public ApplicationId submit(String customerSpace, ProcessAnalyzeRequest request, WorkflowPidWrapper pidWrapper) {
        log.info("WorkflowJob created with pid=" + pidWrapper.getPid());
        if (customerSpace == null) {
            throw new IllegalArgumentException("There is not CustomerSpace in MultiTenantContext");
        }
        DataCollection dataCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace);
        if (dataCollection == null) {
            throw new LedpException(LedpCode.LEDP_37014);
        }

        DataFeed datafeed = dataFeedProxy.getDataFeed(customerSpace);
        Status datafeedStatus = datafeed.getStatus();
        log.info(String.format("data feed %s status: %s", datafeed.getName(), datafeedStatus.getName()));

        if (!dataFeedProxy.lockExecution(customerSpace, DataFeedExecutionJobType.PA)) {
            throw new RuntimeException(String.format("We can't start processAnalyze workflow by dataFeedStatus %s",
                    datafeedStatus.getName()));
        }

        Status initialStatus = getInitialDataFeedStatus(datafeedStatus);

        log.info(String.format("data feed %s initial status: %s", datafeed.getName(), initialStatus.getName()));

        log.info(String.format("Submitting process and analyze workflow for customer %s", customerSpace));

        try {
            Pair<List<Long>, List<Long>> actionAndJobIds = getActionAndJobIds(customerSpace);
            updateActions(actionAndJobIds.getLeft(), pidWrapper.getPid());

            String currentDataCloudBuildNumber = columnMetadataProxy.latestVersion(null).getDataCloudBuildNumber();
            ProcessAnalyzeWorkflowConfiguration configuration = generateConfiguration(customerSpace, request,
                    actionAndJobIds, initialStatus, currentDataCloudBuildNumber);

            configuration.setFailingStep(request.getFailingStep());

            return workflowJobService.submit(configuration, pidWrapper.getPid());
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            dataFeedProxy.failExecution(customerSpace, datafeedStatus.getName());
            throw new RuntimeException(String.format("Failed to submit %s's P&A workflow", customerSpace));
        }
    }

    private Status getInitialDataFeedStatus(Status status) {
        if (status.equals(Status.ProcessAnalyzing)) {
            return Status.Active;
        } else {
            return status;
        }
    }

    @VisibleForTesting
    Pair<List<Long>, List<Long>> getActionAndJobIds(String customerSpace) {
        List<Action> actions = actionService.findByOwnerId(null);
        log.info(String.format("Actions are %s for tenant=%s", Arrays.toString(actions.toArray()), customerSpace));
        Set<ActionType> importAndDeleteTypes = Sets.newHashSet( //
                ActionType.CDL_DATAFEED_IMPORT_WORKFLOW, //
                ActionType.CDL_OPERATION_WORKFLOW);
        // TODO add status filter to filter out running ones
        List<String> importAndDeleteJobIdStrs = actions.stream()
                .filter(action -> importAndDeleteTypes.contains(action.getType()) && action.getTrackingId() != null)
                .map(action -> action.getTrackingId().toString()).collect(Collectors.toList());
        log.info(String.format("importAndDeleteJobIdStrs are %s", importAndDeleteJobIdStrs));
        List<Job> importAndDeleteJobs = workflowProxy.getWorkflowExecutionsByJobIds(importAndDeleteJobIdStrs,
                customerSpace);

        List<Long> completedImportAndDeleteJobIds = CollectionUtils.isEmpty(importAndDeleteJobs)
                ? Collections.emptyList()
                : importAndDeleteJobs.stream().filter(
                        job -> job.getJobStatus() != JobStatus.PENDING && job.getJobStatus() != JobStatus.RUNNING)
                        .map(Job::getId).collect(Collectors.toList());

        log.info(String.format("Jobs that associated with the current consolidate job are: %s",
                completedImportAndDeleteJobIds));

        List<Long> completedActionIds = actions.stream()
                .filter(action -> isCompleteAction(action, importAndDeleteTypes, completedImportAndDeleteJobIds))
                .map(Action::getPid).collect(Collectors.toList());
        log.info(String.format("Actions that associated with the current consolidate job are: %s", completedActionIds));

        List<Long> attrManagementActionIds = actions.stream()
                .filter(action -> ActionType.getAttrManagementTypes().contains(action.getType()))
                .map(Action::getPid).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(attrManagementActionIds)) {
            log.info(String.format("Actions that associated with the Attr management are: %s", attrManagementActionIds));
            completedActionIds.addAll(attrManagementActionIds);
        }

        List<Long> ratingEngineActionIds = actions.stream()
                .filter(action -> action.getType() == ActionType.RATING_ENGINE_CHANGE).map(Action::getPid)
                .collect(Collectors.toList());
        log.info(String.format("RatingEngine related Actions are: %s", ratingEngineActionIds));

        return new ImmutablePair<>(completedActionIds, completedImportAndDeleteJobIds);
    }

    private void updateActions(List<Long> actionIds, Long workflowPid) {
        log.info(String.format("Updating actions=%s with place holder ownerId=%d", Arrays.toString(actionIds.toArray()),
                workflowPid));
        if (CollectionUtils.isNotEmpty(actionIds)) {
            actionService.patchOwnerIdByPids(workflowPid, actionIds);
        }
    }

    private boolean isCompleteAction(Action action, Set<ActionType> selectedTypes,
            List<Long> completedImportAndDeleteJobIds) {
        boolean isComplete = true; // by default every action is valid
        if (selectedTypes.contains(action.getType())) {
            // special check if is selected type
            isComplete = false;
            if (completedImportAndDeleteJobIds.contains(action.getTrackingId())) {
                isComplete = true;
            } else if (ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())) {
                ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action.getActionConfiguration();
                if (Boolean.TRUE.equals(importActionConfiguration.getMockCompleted())) {
                    isComplete = true;
                }
            }
        }
        return isComplete;
    }

    private ProcessAnalyzeWorkflowConfiguration generateConfiguration(String customerSpace,
            ProcessAnalyzeRequest request, Pair<List<Long>, List<Long>> actionAndJobIds, Status status,
            String currentDataCloudBuildNumber) {
        DataCloudVersion dataCloudVersion = columnMetadataProxy.latestVersion(null);
        String scoringQueue = LedpQueueAssigner.getScoringQueueNameForSubmission();

        FeatureFlagValueMap flags = batonService.getFeatureFlags(MultiTenantContext.getCustomerSpace());
        TransformationGroup transformationGroup = FeatureFlagUtils.getTransformationGroupFromZK(flags);
        List<TransformDefinition> stdTransformDefns = UpdateTransformDefinitionsUtils
                .getTransformDefinitions(SchemaInterpretation.SalesforceAccount.toString(), transformationGroup);

        int maxIteration = request.getMaxRatingIterations() != null ? request.getMaxRatingIterations() : 1;
        String apsRollingPeriod = zkConfigService.getRollingPeriod(CustomerSpace.parse(customerSpace)).getPeriodName();

        return new ProcessAnalyzeWorkflowConfiguration.Builder() //
                .microServiceHostPort(microserviceHostPort) //
                .customer(CustomerSpace.parse(customerSpace)) //
                .internalResourceHostPort(internalResourceHostPort) //
                .initialDataFeedStatus(status) //
                .importAndDeleteJobIds(actionAndJobIds.getRight()) //
                .actionIds(actionAndJobIds.getLeft()) //
                .rebuildEntities(request.getRebuildEntities()) //
                .rebuildSteps(request.getRebuildSteps()) //
                .ignoreDataCloudChange(request.getIgnoreDataCloudChange()) //
                .userId(request.getUserId()) //
                .dataCloudVersion(dataCloudVersion) //
                .matchYarnQueue(scoringQueue) //
                .inputProperties(ImmutableMap.<String, String> builder() //
                        .put(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS, status.getName()) //
                        .put(WorkflowContextConstants.Inputs.JOB_TYPE, "processAnalyzeWorkflow") //
                        .put(WorkflowContextConstants.Inputs.DATAFEED_STATUS, status.getName()) //
                        .put(WorkflowContextConstants.Inputs.ACTION_IDS, actionAndJobIds.getLeft().toString()) //
                        .build()) //
                .workflowContainerMem(workflowMemMb) //
                .currentDataCloudBuildNumber(currentDataCloudBuildNumber) //
                .transformationGroup(transformationGroup, stdTransformDefns) //
                .dynamoSignature(signature) //
                .maxRatingIteration(maxIteration) //
                .apsRollingPeriod(apsRollingPeriod) //
                .build();
    }

    public ApplicationId retryLatestFailed(String customerSpace, Integer memory) {
        DataFeed datafeed = dataFeedProxy.getDataFeed(customerSpace);
        Long workflowId = dataFeedProxy.restartExecution(customerSpace, DataFeedExecutionJobType.PA);
        try {
            log.info(String.format("restarted execution with pid: %s", workflowId));
            return workflowJobService.restart(workflowId, customerSpace, memory);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            dataFeedProxy.failExecution(customerSpace, datafeed.getStatus().getName());
            throw new RuntimeException(String.format("Failed to retry %s's P&A workflow", customerSpace));
        }
    }

}
