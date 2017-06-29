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
import com.latticeengines.domain.exposed.serviceflows.cdl.ProfileAndPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Deprecated
@Component
public class CalculateStatsWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(CalculateStatsWorkflowSubmitter.class);

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    public ApplicationId submit() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new IllegalArgumentException("There is not CustomerSpace in MultiTenantContext");
        }
        DataCollection dataCollection = dataCollectionProxy
                .getDefaultDataCollection(MultiTenantContext.getCustomerSpace().toString());
        if (dataCollection == null) {
            throw new LedpException(LedpCode.LEDP_37014);
        }
        log.info("Found default data collection " + dataCollection.getName());
        log.info(String.format("Submitting calculate stats workflow for customer %s", customerSpace));
        DataFeed datafeed = dataFeedProxy.getDataFeed(MultiTenantContext.getCustomerSpace().toString());
        Status datafeedStatus = datafeed.getStatus();
        log.info(String.format("data feed %s status: %s", datafeed.getName(), datafeedStatus.getName()));
        if (datafeedStatus == Status.Active || datafeedStatus == Status.InitialConsolidated) {
            dataFeedProxy.updateDataFeedStatus(customerSpace.toString(), Status.Profiling.getName());
            Table masterTableInDb = dataCollectionProxy.getTable(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedAccount);
            if (masterTableInDb == null) {
                throw new LedpException(LedpCode.LEDP_37003,
                        new String[] { TableRoleInCollection.ConsolidatedAccount.name() });
            }
            ProfileAndPublishWorkflowConfiguration configuration = generateConfiguration(datafeedStatus);
            return workflowJobService.submit(configuration);
        } else {
            throw new RuntimeException(
                    "The status of dataFeed does not meet Finalize Step requirement. We cannot launch Finalize step yet.");
        }

    }

    public ProfileAndPublishWorkflowConfiguration generateConfiguration(Status status) {
        return new ProfileAndPublishWorkflowConfiguration.Builder() //
                .microServiceHostPort(microserviceHostPort) //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .inputProperties(ImmutableMap.<String, String> builder()
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
