package com.latticeengines.pls.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.serviceflows.cdl.CalculateStatsWorkflowConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class CalculateStatsWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(CalculateStatsWorkflowSubmitter.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    public ApplicationId submit(DataCollectionType dataCollectionType) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new IllegalArgumentException("There is not CustomerSpace in MultiTenantContext");
        }
        log.info(String.format("Submitting calculate stats workflow for data collection %s for customer %s",
                dataCollectionType, customerSpace));
        DataCollection dataCollection = dataCollectionProxy.getDataCollectionByType(customerSpace.toString(), dataCollectionType);
        if (dataCollection == null) {
            throw new LedpException(LedpCode.LEDP_37013, new String[] { dataCollectionType.name() });
        }
        if (dataCollection.getTable(SchemaInterpretation.Account.name()) == null) {
            throw new LedpException(LedpCode.LEDP_37003, new String[] { SchemaInterpretation.Account.name() });
        }

        CalculateStatsWorkflowConfiguration configuration = generateConfiguration(dataCollectionType);
        return workflowJobService.submit(configuration);
    }

    public CalculateStatsWorkflowConfiguration generateConfiguration(DataCollectionType dataCollectionType) {
        return new CalculateStatsWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .dataCollectionType(dataCollectionType) //
                .build();
    }

}
