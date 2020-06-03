package com.latticeengines.apps.cdl.workflow;

import java.util.Collection;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.PublishDynamoWorkflowConfiguration;

@Component
public class PublishTableRoleWorkflowSubmitter extends WorkflowSubmitter {

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @Inject
    private DataCollectionService dataCollectionService;

    @WithWorkflowJobPid
    public ApplicationId submitPublishDynamo(@NotNull String customerSpace,
            @NotNull Collection<TableRoleInCollection> tableRoles, DataCollection.Version version,
            @NotNull WorkflowPidWrapper pidWrapper) {
        if (version == null) {
            version = dataCollectionService.getActiveVersion(customerSpace);
        }
        PublishDynamoWorkflowConfiguration configuration = new PublishDynamoWorkflowConfiguration.Builder() //
                .customer(CustomerSpace.parse(customerSpace)) //
                .tableRoles(tableRoles) //
                .version(version) //
                .dynamoSignature(signature) //
                .build();
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

}
