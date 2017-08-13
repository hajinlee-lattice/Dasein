package com.latticeengines.apps.cdl.workflow;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProfileAndPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component
public class ProfileAndPublishWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ProfileAndPublishWorkflowSubmitter.class);

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${cdl.transform.workflow.mem.mb}")
    protected int workflowMemMb;

    private final DataCollectionProxy dataCollectionProxy;

    private final DataFeedProxy dataFeedProxy;

    private final WorkflowProxy workflowProxy;

    @Inject
    public ProfileAndPublishWorkflowSubmitter(DataCollectionProxy dataCollectionProxy, DataFeedProxy dataFeedProxy,
            WorkflowProxy workflowProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.dataFeedProxy = dataFeedProxy;
        this.workflowProxy = workflowProxy;
    }

    public ApplicationId submit(String customerSpace) {
        if (customerSpace == null) {
            throw new IllegalArgumentException("There is not CustomerSpace in MultiTenantContext");
        }
        DataCollection dataCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace);
        if (dataCollection == null) {
            throw new LedpException(LedpCode.LEDP_37014);
        }
        log.info("Found default data collection " + dataCollection.getName());
        log.info(String.format("Submitting profile workflow for customer %s", customerSpace));
        DataFeed datafeed = dataFeedProxy.getDataFeed(customerSpace);
        Status datafeedStatus = datafeed.getStatus();
        log.info(String.format("data feed %s status: %s", datafeed.getName(), datafeedStatus.getName()));
        DataFeedProfile profile = datafeed.getActiveProfile();

        if (!datafeedStatus.isAllowProfile()) {
            throw new RuntimeException("we can't launch any profile workflow now as it is not ready.");
        } else if (profile != null) {
            if (profile.getWorkflowId() == null) {
                throw new RuntimeException("We can't launch any profile workflow now as there is one still running.");
            }
            Job job = workflowProxy.getWorkflowExecution(String.valueOf(profile.getWorkflowId()));
            if (job != null && !job.getJobStatus().isTerminated()) {
                throw new RuntimeException("We can't launch any profile workflow now as there is one still running.");
            }
        }
        Table masterTableInDb = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedAccount);
        if (masterTableInDb == null) {
            throw new LedpException(LedpCode.LEDP_37003,
                    new String[] { TableRoleInCollection.ConsolidatedAccount.name() });
        }
        dataFeedProxy.startProfile(customerSpace);
        ProfileAndPublishWorkflowConfiguration configuration = generateConfiguration(customerSpace, datafeedStatus);
        return workflowJobService.submit(configuration);
    }

    public ProfileAndPublishWorkflowConfiguration generateConfiguration(String customerSpace, Status status) {
        return new ProfileAndPublishWorkflowConfiguration.Builder() //
                .microServiceHostPort(microserviceHostPort) //
                .customer(CustomerSpace.parse(customerSpace)) //
                .internalResourceHostPort(internalResourceHostPort) //
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .inputProperties(ImmutableMap.<String, String> builder()
                        .put(WorkflowContextConstants.Inputs.DATAFEED_STATUS, status.getName()) //
                        .build()) //
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
