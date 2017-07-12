package com.latticeengines.apps.cdl.workflow;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeeingines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.domain.exposed.serviceflows.cdl.ConsolidateAndPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component
public class ConsolidateAndPublishWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ConsolidateAndPublishWorkflowSubmitter.class);

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${cdl.transform.workflow.mem.mb}")
    protected int workflowMemMb;

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
            JobStatus status = workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId()))
                    .getJobStatus();
            if (!status.isTerminated()) {
                throw new RuntimeException(
                        "We can't launch any consolidate workflow now as there is one still running.");
            } else if (JobStatus.FAILED.equals(status)) {
                log.info(String.format(
                        "Execution %s of data feed %s already terminated in an unknown state. Fail this execution so that we can start a new one.",
                        execution, datafeed));
                dataFeedProxy.failExecution(customerSpace, datafeedStatus.getName());
            }
        } else if (execution != null && DataFeedExecution.Status.Failed.equals(execution.getStatus())) {
            log.info("current execution failed, we will start a new one");
        }
        execution = dataFeedProxy.startExecution(customerSpace);
        log.info(String.format("started execution of %s with status: %s", datafeed.getName(), execution.getStatus()));
        WorkflowConfiguration configuration = generateConfiguration(customerSpace, datafeedStatus);
        return workflowJobService.submit(configuration);
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
            JobStatus status = workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId()))
                    .getJobStatus();
            if (JobStatus.FAILED.equals(status)) {
                log.info(String.format(
                        "Execution %s of data feed %s already terminated in an unknown state. Fail this execution so that we can start a new one.",
                        execution, datafeed));
                dataFeedProxy.failExecution(customerSpace, datafeedStatus.getName());
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

    private WorkflowConfiguration generateConfiguration(String customerSpace, Status initialDataFeedStatus) {
        return new ConsolidateAndPublishWorkflowConfiguration.Builder() //
                .initialDataFeedStatus(initialDataFeedStatus) //
                .customer(CustomerSpace.parse(customerSpace)) //
                .microServiceHostPort(microserviceHostPort) //
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .inputProperties(ImmutableMap.<String, String> builder()
                        .put(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS, initialDataFeedStatus.getName()) //
                        .build()) //
                .idField(InterfaceName.LEAccountIDLong.name()) //
                .matchKeyMap(ImmutableMap.<MatchKey, List<String>> builder() //
                        .put(MatchKey.Domain, Collections.singletonList("URL")) //
                        .put(MatchKey.Name, Collections.singletonList("DisplayName")) //
                        .put(MatchKey.City, Collections.singletonList(InterfaceName.City.name())) //
                        .put(MatchKey.State, Collections.singletonList("StateProvince")) //
                        .put(MatchKey.Country, Collections.singletonList(InterfaceName.Country.name())) //
                        .put(MatchKey.Zipcode, Collections.singletonList("Zip")) //
                        .build()) //
                .dropConsolidatedTable(true) //
                .workflowContainerMem(workflowMemMb) //
                .build();
    }

    private HdfsToRedshiftConfiguration createExportBaseConfig() {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setDistStyle(DistStyle.Key);
        redshiftTableConfig.setDistKey(TableRoleInCollection.BucketedAccount.getPrimaryKey().name());
        redshiftTableConfig.setSortKeyType(SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(TableRoleInCollection.BucketedAccount.getForeignKeysAsStringList());
        redshiftTableConfig.setS3Bucket(s3Bucket);
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);
        return exportConfig;
    }
}
