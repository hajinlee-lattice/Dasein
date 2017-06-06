package com.latticeengines.pls.workflow;

import java.util.Collections;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.cdl.workflow.ConsolidateAndPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class ConsolidateAndPublishWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ConsolidateAndPublishWorkflowSubmitter.class);

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    public ApplicationId submit(String datafeedName) {
        WorkflowConfiguration configuration = generateConfiguration(datafeedName);
        ApplicationId applicationId = workflowJobService.submit(configuration);
        return applicationId;

    }

    private WorkflowConfiguration generateConfiguration(String datafeedName) {
        return new ConsolidateAndPublishWorkflowConfiguration.Builder().customer(MultiTenantContext.getCustomerSpace()) //
                .microServiceHostPort(microserviceHostPort) //
                .datafeedName(datafeedName) //
                .hdfsToRedshiftConfiguration(createExportBaseConfig()) //
                .inputProperties(ImmutableMap.<String, String> builder()
                        .put(WorkflowContextConstants.Inputs.DATAFEED_NAME, datafeedName) //
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
