package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.PublishActivityAlertWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component
public class PublishActivityAlertWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(PublishActivityAlertWorkflowSubmitter.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @WithWorkflowJobPid
    public ApplicationId submit(String customerSpace, WorkflowPidWrapper pidWrapper) {

        PublishActivityAlertWorkflowConfiguration config = new PublishActivityAlertWorkflowConfiguration.Builder()
                .customer(CustomerSpace.parse(customerSpace)) //
                .internalResourceHostPort(internalResourceHostPort) //
                .setRebuild(true) //
                .build();

        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        DataCollection.Version inactive = activeVersion.complement();
        // Get the ActivityAlert table
        String activityAlertTable = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ActivityAlert, activeVersion);
        if (StringUtils.isBlank(activityAlertTable)) {
            log.info("PublishActivityAlertWorkflowSubmitter, get alert table from inactive {}", inactive);
            activityAlertTable = dataCollectionProxy.getTableName(customerSpace.toString(),
                    TableRoleInCollection.ActivityAlert, inactive);
        }
        log.info("PublishActivityAlertWorkflowSubmitter, activityAlertTable {}", activityAlertTable);
        // Get DataCollectionStatus
        DataCollectionStatus dcStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace.toString(),
                null);

        Map<String, String> initialContext = new HashMap<>();
        // Set ALERT_GENERATED & ACTIVITY_ALERT_MASTER_TABLE_NAME in context
        initialContext.put("ALERT_GENERATED", "true");
        initialContext.put("ALERT_MASTER_TABLE_NAME", activityAlertTable);
        initialContext.put("CDL_COLLECTION_STATUS", JsonUtils.serialize(dcStatus));
        config.setInitialContext(initialContext);
        log.info("PublishActivityAlertWorkflowSubmitter, initial context {}", config.getInitialContext());

        return workflowJobService.submit(config, pidWrapper.getPid());
    }
}
