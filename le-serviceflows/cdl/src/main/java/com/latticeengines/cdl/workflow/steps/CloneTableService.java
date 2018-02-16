package com.latticeengines.cdl.workflow.steps;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;

@Component
public class CloneTableService {

    private static final Logger log = LoggerFactory.getLogger(CloneTableService.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${dataplatform.queue.scheme}")
    private String queueScheme;

    private CustomerSpace customerSpace;
    private DataCollection.Version active;
    private DataCollection.Version inactive;

    public void setActiveVersion(DataCollection.Version active) {
        this.active = active;
        this.inactive = active.complement();
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public void linkInactiveTable(TableRoleInCollection role) {
        linkOrCloneToInactiveTable(role, false);
    }

    public void cloneToInactiveTable(TableRoleInCollection role) {
        linkOrCloneToInactiveTable(role, true);
    }

    private void linkOrCloneToInactiveTable(TableRoleInCollection role, boolean clone) {
        String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
        String inactiveTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
        if (StringUtils.isBlank(activeTableName)) {
            log.info("There is no " + role + " table in active version, skip cloning.");
            return;
        } else if (StringUtils.isNotBlank(inactiveTableName)) {
            log.info("There is already " + role + " table in inactive version, skip cloning.");
            return;
        }
        if (clone) {
            log.info("Cloning " + role + " from  " + active + " to " + inactive);
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
        } else {
            log.info("Linking " + role + " from " + active + " to " + inactive);
            dataCollectionProxy.upsertTable(customerSpace.toString(), activeTableName, role, inactive);
        }
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
        for (BusinessEntity entity : BusinessEntity.values()) {
            if (role.equals(entity.getServingStore())) {
                return entity;
            }
        }
        return null;
    }

}
