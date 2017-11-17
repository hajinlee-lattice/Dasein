package com.latticeengines.apps.cdl.workflow;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.period.PeriodStrategy;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.ConsolidateAndPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component
public class ConsolidateAndPublishWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateAndPublishWorkflowSubmitter.class);

    private final DataCollectionProxy dataCollectionProxy;

    private final DataFeedProxy dataFeedProxy;

    private final WorkflowProxy workflowProxy;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${cdl.transform.workflow.mem.mb}")
    protected int workflowMemMb;

    @Inject
    public ConsolidateAndPublishWorkflowSubmitter(DataCollectionProxy dataCollectionProxy, DataFeedProxy dataFeedProxy,
            WorkflowProxy workflowProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.dataFeedProxy = dataFeedProxy;
        this.workflowProxy = workflowProxy;
    }

    public ApplicationId submit(String customerSpace) {
        DataFeed datafeed = dataFeedProxy.getDataFeed(customerSpace);
        log.info(String.format("data feed %s status: %s", datafeed.getName(), datafeed.getStatus()));
        DataFeedExecution execution = datafeed.getActiveExecution();

        Status datafeedStatus = datafeed.getStatus();
        if (!datafeedStatus.isAllowConsolidation()) {
            throw new RuntimeException("We can't launch any consolidate workflow now as it is not ready.");
        } else if (execution != null && DataFeedExecution.Status.Started.equals(execution.getStatus())) {
            if (execution.getWorkflowId() == null) {
                throw new RuntimeException(
                        "We can't launch any consolidate workflow now as there is one still running.");
            }
            Job job = workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId()));
            JobStatus status = job.getJobStatus();
            if (!status.isTerminated()) {
                throw new RuntimeException(
                        "We can't launch any consolidate workflow now as there is one still running.");
            } else if (JobStatus.FAILED.equals(status)) {
                log.info(String.format(
                        "Execution %s of data feed %s already terminated in an unknown state. Fail this execution so that we can start a new one.",
                        execution, datafeed));
                dataFeedProxy.failExecution(customerSpace,
                        job.getInputs().get(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS));
            }
        } else if (execution != null && DataFeedExecution.Status.Failed.equals(execution.getStatus())) {
            log.info("current execution failed, we will start a new one");
        }
        execution = dataFeedProxy.startExecution(customerSpace);
        log.info(String.format("started execution of %s with status: %s", datafeed.getName(), execution.getStatus()));
        List<Long> importJobIds = getImportJobIds(customerSpace);
        log.info(String.format("importJobIdsStr=%s", importJobIds));
        WorkflowConfiguration configuration = generateConfiguration(customerSpace, datafeedStatus, importJobIds);
        return workflowJobService.submit(configuration);
    }

    @VisibleForTesting
    List<Long> getImportJobIds(String customerSpace) {
        List<Job> importJobs = workflowProxy.getWorkflowJobs(customerSpace, null,
                Arrays.asList("cdlDataFeedImportWorkflow"), null, Boolean.FALSE);
        List<Long> importJobIds = Collections.emptyList();
        if (CollectionUtils.isEmpty(importJobs)) {
            log.warn("No import jobs are associated with the current consolidate job");
        } else {
            importJobIds = importJobs.stream().map(job -> job.getId()).collect(Collectors.toList());
            log.info(String.format("Import jobs that associated with the current consolidate job are: %s",
                    importJobIds));
        }
        return importJobIds;
    }

    public ApplicationId retryLatestFailed(String customerSpace) {
        DataFeed datafeed = dataFeedProxy.getDataFeed(customerSpace);
        log.info(String.format("data feed status: %s", datafeed.getStatus()));
        DataFeedExecution execution = datafeed.getActiveExecution();

        Status datafeedStatus = datafeed.getStatus();
        if (execution == null || !datafeedStatus.isAllowConsolidation()) {
            throw new RuntimeException("We can't launch any consolidate workflow now as it is not ready.");
        } else if (DataFeedExecution.Status.Started.equals(execution.getStatus())) {
            if (execution.getWorkflowId() == null) {
                throw new RuntimeException("We can't retry any consolidate workflow now as we can't find workflow id.");
            }
            Job job = workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId()));
            JobStatus status = job.getJobStatus();
            if (JobStatus.FAILED.equals(status)) {
                log.info(String.format(
                        "Execution %s of data feed %s already terminated in an unknown state. Fail this execution so that we can start a new one.",
                        execution, datafeed));
                dataFeedProxy.failExecution(customerSpace,
                        job.getInputs().get(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS));
            } else {
                throw new RuntimeException(
                        "We can't restart consolidate workflow as the most recent one is not failed");
            }
        } else if (!DataFeedExecution.Status.Failed.equals(execution.getStatus())) {
            throw new RuntimeException("We can't restart consolidate workflow as the most recent one is not failed");
        }
        execution = dataFeedProxy.retryLatestExecution(customerSpace);
        log.info(String.format("restarted execution with status: %s", execution.getStatus()));
        return workflowJobService.restart(execution.getWorkflowId());
    }

    private WorkflowConfiguration generateConfiguration(String customerSpace, Status initialDataFeedStatus,
            List<Long> importJobIds) {
        return new ConsolidateAndPublishWorkflowConfiguration.Builder() //
                .initialDataFeedStatus(initialDataFeedStatus) //
                .customer(CustomerSpace.parse(customerSpace)) //
                .microServiceHostPort(microserviceHostPort) //
                .internalResourceHostPort(internalResourceHostPort)
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .importJobIds(importJobIds) //
                .inputProperties(ImmutableMap.<String, String> builder()
                        .put(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS, initialDataFeedStatus.getName()) //
                        .put(WorkflowContextConstants.Inputs.CHILDREN_WORKFLOW_JOB_IDS, importJobIds.toString()) //
                        .build()) //
                .accountIdField(InterfaceName.Id.name()) //
                .bucketAccount(dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.Profile) != null) //
                .contactIdField(InterfaceName.Id.name()) //
                .productIdField(InterfaceName.Id.name()) //
                .bucketContact(dataCollectionProxy.getTable(customerSpace, //
                        TableRoleInCollection.ContactProfile) != null) //
                .transactionIdField(InterfaceName.Id.name()) //
                .periodStrategy(PeriodStrategy.CalendarMonth) //
                .bucketTransaction(true) //
                .bucketProduct(true) //
                .workflowContainerMem(workflowMemMb) //
                .build();
    }

    private HdfsToRedshiftConfiguration createExportBaseConfig() {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        exportConfig.setCreateNew(false);
        exportConfig.setAppend(false);
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setS3Bucket(s3Bucket);
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);
        return exportConfig;
    }
}
