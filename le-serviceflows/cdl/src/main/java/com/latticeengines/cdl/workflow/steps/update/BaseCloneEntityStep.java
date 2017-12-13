package com.latticeengines.cdl.workflow.steps.update;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;

public abstract class BaseCloneEntityStep<T extends ProcessStepConfiguration> extends BaseWorkflowStep<T> {

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;

    @Value("${dataplatform.queue.scheme}")
    private String queueScheme;

    @Override
    public void execute() {
        customerSpace = CustomerSpace.parse(getObjectFromContext(CUSTOMER_SPACE, String.class));
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        for (TableRoleInCollection role: tablesToClone()) {
            String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
            if (StringUtils.isNotBlank(activeTableName)) {
                cloneToInactiveTable(role);
            }
        }

    }

    private void cloneToInactiveTable(TableRoleInCollection role) {
        log.info("Cloning " + role + " from  " + active + " to " + inactive);
        String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
        Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
        String cloneName = NamingUtils.timestamp(role.name());
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);
        Table inactiveTable = TableCloneUtils //
                .cloneDataTable(yarnConfiguration, customerSpace, cloneName, activeTable, queue);
        metadataProxy.createTable(customerSpace.toString(), cloneName, inactiveTable);
        if (isServingStore(role)) {
            BusinessEntity entity = servingStoreEntity(role);
            String prefix = String.join("_", customerSpace.getTenantId(), entity.name());
            cloneName = NamingUtils.timestamp(prefix);
            metadataProxy.updateTable(customerSpace.toString(), cloneName, inactiveTable);
            copyRedshiftTable(activeTableName, cloneName);
        }
        dataCollectionProxy.upsertTable(customerSpace.toString(), cloneName, role, inactive);
    }

    private void copyRedshiftTable(String original, String clone) {
        redshiftService.dropTable(clone);
        if (redshiftService.hasTable(original)) {
            redshiftService.cloneTable(original, clone);
        }
    }

    private boolean isServingStore(TableRoleInCollection role) {
        return servingStoreEntity(role) != null;
    }

    private BusinessEntity servingStoreEntity(TableRoleInCollection role) {
        for (BusinessEntity entity: BusinessEntity.values()) {
            if (role.equals(entity.getServingStore())) {
                return entity;
            }
        }
        return null;
    }

    protected abstract List<TableRoleInCollection> tablesToClone();

}
