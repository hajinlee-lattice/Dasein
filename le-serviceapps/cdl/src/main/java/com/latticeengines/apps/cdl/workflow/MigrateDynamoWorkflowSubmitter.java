package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MigrateDynamoRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.MigrateDynamoWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component
public class MigrateDynamoWorkflowSubmitter extends WorkflowSubmitter {

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull CustomerSpace customerSpace, @NotNull MigrateDynamoRequest request,
                                @NotNull WorkflowPidWrapper pidWrapper) {
        MigrateDynamoWorkflowConfiguration configuration = configure(customerSpace, request);
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(MigrateDynamoWorkflowConfiguration.IMPORT_TABLE_NAMES, JsonUtils.serialize(request.getTableNames()));
        configuration.setInputProperties(inputProperties);
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private MigrateDynamoWorkflowConfiguration configure(CustomerSpace customerSpace, MigrateDynamoRequest request) {
        return new MigrateDynamoWorkflowConfiguration.Builder().customer(customerSpace).tableNames(request.getTableNames())
                .dynamoSignature(signature).migrateTable(Boolean.TRUE).build();
    }
}
