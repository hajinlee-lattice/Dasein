package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;

@Component("clonePeriodStores")
public class ClonePeriodStores extends BaseWorkflowStep<ProcessTransactionStepConfiguration> {

    protected CustomerSpace customerSpace;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Value("${dataplatform.queue.scheme}")
    protected String queueScheme;

    @Override
    public void execute() {
        customerSpace = CustomerSpace.parse(getObjectFromContext(CUSTOMER_SPACE, String.class));
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        clonePeriodStore(TableRoleInCollection.ConsolidatedRawTransaction);
        clonePeriodStore(TableRoleInCollection.ConsolidatedDailyTransaction);
        clonePeriodStore(TableRoleInCollection.ConsolidatedPeriodTransaction);
    }

    private void clonePeriodStore(TableRoleInCollection role) {
        String inactiveTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
        if (StringUtils.isBlank(inactiveTableName)) {
            String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
            if (StringUtils.isNotBlank(activeTableName)) {
                log.info("Cloning " + role + " from " + active + " to " + inactive);
                Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
                String cloneName = NamingUtils.timestamp(role.name());
                String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
                queue = LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);
                Table inactiveTable = TableCloneUtils //
                        .cloneDataTable(yarnConfiguration, customerSpace, cloneName, activeTable, queue);
                metadataProxy.createTable(customerSpace.toString(), cloneName, inactiveTable);
                dataCollectionProxy.upsertTable(customerSpace.toString(), cloneName, role, inactive);
            }
        }
    }

}
