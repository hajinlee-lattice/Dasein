package com.latticeengines.pls.workflow;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.domain.exposed.serviceflows.cdl.ConsolidateAndPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Deprecated
@Component
public class ConsolidateAndPublishWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ConsolidateAndPublishWorkflowSubmitter.class);

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    public ApplicationId submit() {
        DataFeed datafeed = dataFeedProxy.getDataFeed(MultiTenantContext.getCustomerSpace().toString());
        log.info(String.format("data feed %s status: %s", datafeed.getName(), datafeed.getStatus()));
        DataFeedExecution execution = datafeed.getActiveExecution();

        Status datafeedStatus = datafeed.getStatus();
        if (!datafeedStatus.isAllowConsolidation()) {
            throw new RuntimeException("we can't launch any consolidate workflow now as it is not ready.");
        } else if (execution != null && execution.getStatus() == DataFeedExecution.Status.Started) {
            if (execution.getWorkflowId() == null //
                    || !workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId())).getJobStatus()
                            .isTerminated()) {
                throw new RuntimeException(
                        "we can't launch any consolidate workflow now as there is one already running.");
            } else {
                log.info(String.format(
                        "Execution %s of data feed %s already terminated in an unknown state. Fail this execution so that we can start a new one.",
                        execution, datafeed));
                dataFeedProxy.failExecution(MultiTenantContext.getCustomerSpace().toString(), datafeedStatus.getName());
            }
        }
        execution = dataFeedProxy.startExecution(MultiTenantContext.getCustomerSpace().toString());
        log.info(String.format("started execution of %s with status: %s", datafeed.getName(), execution.getStatus()));
        WorkflowConfiguration configuration = generateConfiguration(datafeed.getName(), datafeedStatus);
        return workflowJobService.submit(configuration);
    }

    public ApplicationId retryLatestFailed() {
        DataFeed datafeed = dataFeedProxy.getDataFeed(MultiTenantContext.getCustomerSpace().toString());
        log.info(String.format("data feed status: %s", datafeed.getStatus()));
        DataFeedExecution execution = datafeed.getActiveExecution();

        Status datafeedStatus = datafeed.getStatus();
        if (!datafeedStatus.isAllowConsolidation()) {
            throw new RuntimeException("we can't launch any consolidate workflow now as it is not ready.");
        } else if (execution != null && execution.getStatus() == DataFeedExecution.Status.Started) {
            if (execution.getWorkflowId() == null //
                    || !workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId())).getJobStatus()
                            .isTerminated()) {
                throw new RuntimeException(
                        "we can't launch any consolidate workflow now as there is one already running.");
            } else {
                log.info(String.format(
                        "Execution %s of data feed %s already terminated in an unknown state. Fail this execution so that we can start a new one.",
                        execution, datafeed));
                dataFeedProxy.failExecution(MultiTenantContext.getCustomerSpace().toString(), datafeedStatus.getName());
            }
        } else if (execution == null || execution.getStatus() != DataFeedExecution.Status.Failed) {
            throw new RuntimeException("we can't retart consolidate workflow as the most recent one is not failed");
        }
        if (execution.getWorkflowId() == null) {
            throw new RuntimeException("we can't retart consolidate workflow as the last workflow has fatal error!");
        }
        execution = dataFeedProxy.retryLatestExecution(MultiTenantContext.getCustomerSpace().toString());
        log.info(String.format("restarted execution of %s with status: %s", execution.getStatus()));
        return workflowJobService.restart(execution.getWorkflowId());
    }

    private WorkflowConfiguration generateConfiguration(String datafeedName, Status initialDataFeedStatus) {
        return new ConsolidateAndPublishWorkflowConfiguration.Builder() //
                .initialDataFeedStatus(initialDataFeedStatus) //
                .customer(MultiTenantContext.getCustomerSpace()) //
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
