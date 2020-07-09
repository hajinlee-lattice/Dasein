package com.latticeengines.apps.cdl.workflow;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.PublishAccountLookupWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component
public class PublishAccountLookupWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(PublishAccountLookupWorkflowSubmitter.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Value("${eai.export.dynamo.accountlookup.signature}")
    private String accountLookupSignature;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull CustomerSpace customerSpace, String signature, WorkflowPidWrapper pidWrapper) {
        log.info("Received request to re-publish account lookup for tenant {}", customerSpace.getTenantId());
        String lookupTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.AccountLookup);
        if (StringUtils.isBlank(lookupTableName)) {
            throw new UnsupportedOperationException(
                    String.format("No lookup table found for tenant %s", customerSpace.getTenantId()));
        }
        log.info("Using lookup table {} to publish new version of lookup entries for tenant {}", lookupTableName,
                customerSpace.getTenantId());
        PublishAccountLookupWorkflowConfiguration configuration = new PublishAccountLookupWorkflowConfiguration.Builder()
                .customeSpace(customerSpace).internalResourceHostPort(internalResourceHostPort)
                .microServiceHostPort(microserviceHostPort)
                .dynamoSignature(StringUtils.isBlank(signature) ? accountLookupSignature : signature).build();
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }
}
