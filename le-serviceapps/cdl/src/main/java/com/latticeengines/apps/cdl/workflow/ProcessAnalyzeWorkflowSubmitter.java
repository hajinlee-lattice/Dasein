package com.latticeengines.apps.cdl.workflow;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
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
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class ProcessAnalyzeWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeWorkflowSubmitter.class);

    // Special owner id for actions whose actual owner Id is not known yet
    public static final Long UNKNOWN_OWNER_ID = 0L;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${cdl.transform.workflow.mem.mb}")
    protected int workflowMemMb;

    private final DataCollectionProxy dataCollectionProxy;

    private final DataFeedProxy dataFeedProxy;

    private final WorkflowProxy workflowProxy;

    private final ColumnMetadataProxy columnMetadataProxy;

    @Inject
    public ProcessAnalyzeWorkflowSubmitter(DataCollectionProxy dataCollectionProxy, DataFeedProxy dataFeedProxy, //
            WorkflowProxy workflowProxy, ColumnMetadataProxy columnMetadataProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.dataFeedProxy = dataFeedProxy;
        this.workflowProxy = workflowProxy;
        this.columnMetadataProxy = columnMetadataProxy;
    }

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public ApplicationId submit(String customerSpace, ProcessAnalyzeRequest request) {
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
            throw new RuntimeException("We can't start processanalyze workflow right now");
        }
        log.info(String.format("Submitting process and analyze workflow for customer %s", customerSpace));

        try {
            Pair<List<Long>, List<Long>> actionAndJobIds = getActionAndJobIds(customerSpace);
            updateActions(customerSpace, actionAndJobIds.getLeft());

            String currentDataCloudBuildNumber = columnMetadataProxy.latestVersion(null).getDataCloudBuildNumber();
            ProcessAnalyzeWorkflowConfiguration configuration = generateConfiguration(customerSpace, request,
                    actionAndJobIds, datafeedStatus, currentDataCloudBuildNumber);

            configuration.setFailingStep(request.getFailingStep());

            return workflowJobService.submit(configuration);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            dataFeedProxy.failExecution(customerSpace, datafeedStatus.getName());
            throw new RuntimeException(String.format("Failed to submit %s's P&A workflow", customerSpace));
        }
    }

    @VisibleForTesting
    Pair<List<Long>, List<Long>> getActionAndJobIds(String customerSpace) {
        List<Action> actions = internalResourceProxy.getActionsByOwnerId(customerSpace, null);
        log.info(String.format("Actions are %s for tenant=%s", Arrays.toString(actions.toArray()), customerSpace));
        Set<ActionType> importAndDeleteTypes = Stream
                .of(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW, ActionType.CDL_OPERATION_WORKFLOW)
                .collect(Collectors.toSet());
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
                        .map(job -> job.getId()).collect(Collectors.toList());

        log.info(String.format("Jobs that associated with the current consolidate job are: %s",
                completedImportAndDeleteJobIds));

        List<Long> completedActionIds = actions.stream()
                .filter(action -> isCompleteAction(action, importAndDeleteTypes, completedImportAndDeleteJobIds))
                .map(action -> action.getPid()).collect(Collectors.toList());
        log.info(String.format("Actions that associated with the current consolidate job are: %s", completedActionIds));

        List<Long> ratingEngineActionIds = actions.stream()
                .filter(action -> action.getType() == ActionType.RATING_ENGINE_CHANGE).map(action -> action.getPid())
                .collect(Collectors.toList());
        log.info(String.format("RatingEngine related Actions are: %s", ratingEngineActionIds));

        Pair<List<Long>, List<Long>> idPair = new ImmutablePair<>(completedActionIds, completedImportAndDeleteJobIds);
        return idPair;
    }

    // update actions with place holder owner Id to minimize the discrepancy
    // issue of jobs page in UI
    private void updateActions(String customerSpace, List<Long> actionIds) {
        log.info(String.format("Updating actions=%s with place holder ownerId=%d", Arrays.toString(actionIds.toArray()),
                UNKNOWN_OWNER_ID));
        if (CollectionUtils.isNotEmpty(actionIds)) {
            internalResourceProxy.updateOwnerIdIn(customerSpace, UNKNOWN_OWNER_ID, actionIds);
        }
    }

    private boolean isCompleteAction(Action action, Set<ActionType> selectedTypes,
            List<Long> completedImportAndDeleteJobIds) {
        if (selectedTypes.contains(action.getType())
                && !completedImportAndDeleteJobIds.contains(action.getTrackingId())) {
            return false;
        }
        return true;
    }

    private ProcessAnalyzeWorkflowConfiguration generateConfiguration(String customerSpace,
            ProcessAnalyzeRequest request, Pair<List<Long>, List<Long>> actionAndJobIds, Status status,
            String currentDataCloudBuildNumber) {
        DataCloudVersion dataCloudVersion = columnMetadataProxy.latestVersion(null);
        String scoringQueue = LedpQueueAssigner.getScoringQueueNameForSubmission();
        return new ProcessAnalyzeWorkflowConfiguration.Builder() //
                .microServiceHostPort(microserviceHostPort) //
                .customer(CustomerSpace.parse(customerSpace)) //
                .internalResourceHostPort(internalResourceHostPort) //
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .initialDataFeedStatus(status) //
                .importAndDeleteJobIds(actionAndJobIds.getRight()) //
                .actionIds(actionAndJobIds.getLeft()) //
                .rebuildEntities(request.getRebuildEntities()) //
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
                .build();
    }

    private HdfsToRedshiftConfiguration createExportBaseConfig() {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        exportConfig.setCreateNew(true);
        exportConfig.setAppend(true);
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setS3Bucket(s3Bucket);
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);
        return exportConfig;
    }

    public ApplicationId retryLatestFailed(String customerSpace) {
        DataFeed datafeed = dataFeedProxy.getDataFeed(customerSpace);
        Long workflowId = dataFeedProxy.restartExecution(customerSpace, DataFeedExecutionJobType.PA);
        try {
            log.info(String.format("restarted execution with pid: %s", workflowId));
            return workflowJobService.restart(workflowId, customerSpace);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            dataFeedProxy.failExecution(customerSpace, datafeed.getStatus().getName());
            throw new RuntimeException(String.format("Failed to retry %s's P&A workflow", customerSpace));
        }
    }

}
