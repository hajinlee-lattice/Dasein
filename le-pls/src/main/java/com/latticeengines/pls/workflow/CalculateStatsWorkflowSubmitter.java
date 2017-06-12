package com.latticeengines.pls.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.CalculateStatsWorkflowConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class CalculateStatsWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(CalculateStatsWorkflowSubmitter.class);

    @Autowired
    private MetadataProxy metadataProxy;

    public ApplicationId submit(String masterTableName) {

        log.info(String.format("Submitting calculate stats workflow for masterTableName %s for customer %s",
                masterTableName, MultiTenantContext.getCustomerSpace()));

        if (metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), masterTableName) == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { masterTableName });
        }

        CalculateStatsWorkflowConfiguration configuration = generateConfiguration(masterTableName);
        return workflowJobService.submit(configuration);
    }

    public CalculateStatsWorkflowConfiguration generateConfiguration(String masterTableName) {
        return new CalculateStatsWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .masterTableName(masterTableName).build();
    }

}
