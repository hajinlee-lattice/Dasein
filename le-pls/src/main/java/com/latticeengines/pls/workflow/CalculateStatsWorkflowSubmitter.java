package com.latticeengines.pls.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.domain.exposed.serviceflows.cdl.CalculateStatsWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class CalculateStatsWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(CalculateStatsWorkflowSubmitter.class);

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    public ApplicationId submit(String dataCollectionName, String datafeedName) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new IllegalArgumentException("There is not CustomerSpace in MultiTenantContext");
        }
        log.info(String.format("Submitting calculate stats workflow for data collection %s for customer %s",
                dataCollectionName, customerSpace));

        DataFeed datafeed = metadataProxy.findDataFeedByName(MultiTenantContext.getCustomerSpace().toString(),
                datafeedName);
        Status datafeedStatus = datafeed.getStatus();
        log.info(String.format("data feed %s status: %s", datafeedName, datafeedStatus.getName()));

        if (datafeedStatus == Status.Active || datafeedStatus == Status.InitialConsolidated) {
            metadataProxy.updateDataFeedStatus(customerSpace.toString(), datafeedName, Status.Finalizing.getName());
            DataCollection dataCollection = dataCollectionProxy.getDataCollection(customerSpace.toString(),
                    dataCollectionName);
            if (dataCollection == null) {
                throw new LedpException(LedpCode.LEDP_37013, new String[] { dataCollection.getType().name() });
            }
            Table masterTableInDb = dataCollectionProxy.getTable(customerSpace.toString(), dataCollectionName,
                    TableRoleInCollection.ConsolidatedAccount);
            if (masterTableInDb == null) {
                throw new LedpException(LedpCode.LEDP_37003,
                        new String[] { TableRoleInCollection.ConsolidatedAccount.name() });
            }
            CalculateStatsWorkflowConfiguration configuration = generateConfiguration(dataCollectionName, datafeedName,
                    datafeedStatus);
            return workflowJobService.submit(configuration);
        } else {
            throw new RuntimeException(
                    "The status of dataFeed does not meet Finalize Step requirement. We cannot launch Finalize step yet.");
        }

    }

    public CalculateStatsWorkflowConfiguration generateConfiguration(String collectionName, String datafeedName,
            Status status) {
        return new CalculateStatsWorkflowConfiguration.Builder() //
                .microServiceHostPort(microserviceHostPort) //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .dataCollectionName(collectionName) //
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .inputProperties(ImmutableMap.<String, String> builder()
                        .put(WorkflowContextConstants.Inputs.DATAFEED_NAME, datafeedName) //
                        .put(WorkflowContextConstants.Inputs.DATAFEED_STATUS, status.getName()) //
                        .build()) //
                .build();
    }

    private HdfsToRedshiftConfiguration createExportBaseConfig() {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        exportConfig.setAppend(true);
        exportConfig.setCreateNew(true);
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
