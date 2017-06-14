package com.latticeengines.pls.workflow;

import java.util.Arrays;
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
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.domain.exposed.serviceflows.cdl.ConsolidateAndPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class ConsolidateAndPublishWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ConsolidateAndPublishWorkflowSubmitter.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    public ApplicationId submit(DataCollectionType dataCollectionType, String datafeedName) {
        DataFeed datafeed = metadataProxy.findDataFeedByName(MultiTenantContext.getCustomerSpace().toString(),
                datafeedName);
        log.info(String.format("data feed %s status: %s", datafeedName, datafeed.getStatus()));
        DataFeedExecution execution = datafeed.getActiveExecution();
        if (datafeed.getStatus() != Status.InitialLoaded && datafeed.getStatus() != Status.Active
                || execution.getStatus() == DataFeedExecution.Status.Started) {
            throw new RuntimeException("we can't launch any consolidate workflow now as it is not ready.");
        }
        execution = metadataProxy.startExecution(MultiTenantContext.getCustomerSpace().toString(), datafeedName);
        log.info(String.format("started execution of %s with status: %s", datafeedName, execution.getStatus()));
        WorkflowConfiguration configuration = generateConfiguration(dataCollectionType, datafeedName);
        ApplicationId applicationId = workflowJobService.submit(configuration);
        return applicationId;
    }

    private WorkflowConfiguration generateConfiguration(DataCollectionType dataCollectionType, String datafeedName) {
        return new ConsolidateAndPublishWorkflowConfiguration.Builder().customer(MultiTenantContext.getCustomerSpace()) //
                .microServiceHostPort(microserviceHostPort) //
                .datafeedName(datafeedName) //
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .inputProperties(ImmutableMap.<String, String> builder()
                        .put(WorkflowContextConstants.Inputs.DATAFEED_NAME, datafeedName) //
                        .build()) //
                .dataCollectionType(dataCollectionType) //
                .idField("LEAccountIDLong") //
                .matchKeyMap(ImmutableMap.<MatchKey, List<String>> builder()
                        .put(MatchKey.Domain, Arrays.asList(InterfaceName.Domain.name())) //
                        .put(MatchKey.Country, Arrays.asList(InterfaceName.Country.name())) //
                        .put(MatchKey.Zipcode, Arrays.asList("Zip")) //
                        .build()) //
                .build();
    }

    private HdfsToRedshiftConfiguration createExportBaseConfig() {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setDistStyle(DistStyle.Key);
        redshiftTableConfig.setDistKey("LatticeAccountId");
        redshiftTableConfig.setSortKeyType(SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(Collections.<String> singletonList("LatticeAccountId"));
        redshiftTableConfig.setS3Bucket(s3Bucket);
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);
        return exportConfig;
    }
}
