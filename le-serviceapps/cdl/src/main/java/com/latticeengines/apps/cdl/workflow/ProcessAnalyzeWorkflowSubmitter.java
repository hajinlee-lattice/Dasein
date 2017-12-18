package com.latticeengines.apps.cdl.workflow;

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

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component
public class ProcessAnalyzeWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeWorkflowSubmitter.class);

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${cdl.transform.workflow.mem.mb}")
    protected int workflowMemMb;

    private final DataCollectionProxy dataCollectionProxy;

    private final DataFeedProxy dataFeedProxy;

    private final WorkflowProxy workflowProxy;

    @Inject
    public ProcessAnalyzeWorkflowSubmitter(DataCollectionProxy dataCollectionProxy, DataFeedProxy dataFeedProxy, //
            WorkflowProxy workflowProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.dataFeedProxy = dataFeedProxy;
        this.workflowProxy = workflowProxy;
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

        DataFeedExecution execution = datafeed.getActiveExecution();
        if (execution != null && DataFeedExecution.Status.Started.equals(execution.getStatus())) {
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

        log.info(String.format("Submitting process and analyze workflow for customer %s", customerSpace));

        execution = dataFeedProxy.startExecution(customerSpace);
        log.info(String.format("started execution of %s with status: %s", datafeed.getName(), execution.getStatus()));
        List<Long> importJobIds = getImportJobIds(customerSpace);
        log.info(String.format("importJobIdsStr=%s", importJobIds));

        ProcessAnalyzeWorkflowConfiguration configuration = generateConfiguration(customerSpace, request, importJobIds,
                datafeedStatus);
        return workflowJobService.submit(configuration);
    }

    private List<Long> getImportJobIds(String customerSpace) {
        List<Job> importJobs = workflowProxy.getWorkflowJobs(customerSpace, null,
                Collections.singletonList("cdlDataFeedImportWorkflow"), null, Boolean.FALSE);
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

    private ProcessAnalyzeWorkflowConfiguration generateConfiguration(String customerSpace,
            ProcessAnalyzeRequest request, List<Long> importJobIds, Status status) {
        return new ProcessAnalyzeWorkflowConfiguration.Builder() //
                .microServiceHostPort(microserviceHostPort) //
                .customer(CustomerSpace.parse(customerSpace)) //
                .internalResourceHostPort(internalResourceHostPort) //
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .initialDataFeedStatus(status) //
                .importJobIds(importJobIds) //
                .rebuildEntities(request.getRebuildEntities()) //
                .inputProperties(ImmutableMap //
                        .of(WorkflowContextConstants.Inputs.DATAFEED_STATUS, status.getName())) //
                .workflowContainerMem(workflowMemMb) //
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

}
