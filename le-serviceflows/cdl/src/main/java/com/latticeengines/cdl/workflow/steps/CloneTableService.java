package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;
import com.latticeengines.yarn.exposed.service.EMREnvService;

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

    @Inject
    private EMREnvService emrEnvService;

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
        List<String> activeTableNames = dataCollectionProxy.getTableNames(customerSpace.toString(), role, active);
        List<String> inactiveTableNames = dataCollectionProxy.getTableNames(customerSpace.toString(), role, inactive);
        if (CollectionUtils.isEmpty(activeTableNames)) {
            log.info("There is no " + role + " table in active version, skip cloning.");
            return;
        } else if (CollectionUtils.isNotEmpty(inactiveTableNames)) {
            log.info("There is already " + role + " table in inactive version, skip cloning.");
            return;
        }
        if (clone) {
            log.info("Cloning role " + role + " from  " + active + " to " + inactive);
            if (role == TableRoleInCollection.ConsolidatedPeriodTransaction) {  // Clone multi-tables
                List<Table> activeTables = dataCollectionProxy.getTables(customerSpace.toString(), role, active);
                List<String> cloneTableNames = new ArrayList<>();
                for (Table activeTable : activeTables) {
                    String periodStrategyName = PeriodStrategyUtils
                            .getPeriodStrategyNameFromPeriodTableName(activeTable.getName(), role);
                    String cloneName = PeriodStrategyUtils.getTablePrefixFromPeriodStrategyName(periodStrategyName)
                            + NamingUtils.timestamp(role.name());
                    String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
                    queue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
                    Table inactiveTable = TableCloneUtils //
                            .cloneDataTable(yarnConfiguration, customerSpace, cloneName, activeTable, queue);
                    metadataProxy.createTable(customerSpace.toString(), cloneName, inactiveTable);
                    cloneTableNames.add(inactiveTable.getName());
                    log.info("Cloned " + activeTable.getName() + " to " + inactiveTable.getName());
                }
                dataCollectionProxy.upsertTables(customerSpace.toString(), cloneTableNames, role, inactive);
            } else {
                Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
                String cloneName = NamingUtils.timestamp(role.name());
                String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
                queue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
                Table inactiveTable = TableCloneUtils //
                        .cloneDataTable(yarnConfiguration, customerSpace, cloneName, activeTable, queue);
                metadataProxy.createTable(customerSpace.toString(), cloneName, inactiveTable);
                if (isServingStore(role)) {
                    BusinessEntity entity = servingStoreEntity(role);
                    String prefix = String.join("_", customerSpace.getTenantId(), entity.name());
                    cloneName = NamingUtils.timestamp(prefix);
                    metadataProxy.updateTable(customerSpace.toString(), cloneName, inactiveTable);
                    copyRedshiftTable(activeTableNames.get(0), cloneName);
                }
                dataCollectionProxy.upsertTable(customerSpace.toString(), cloneName, role, inactive);
            }
        } else {
            log.info("Linking " + role + " from " + active + " to " + inactive);
            if (role == TableRoleInCollection.ConsolidatedPeriodTransaction) {
                dataCollectionProxy.upsertTables(customerSpace.toString(), activeTableNames, role, inactive);
            } else {
                dataCollectionProxy.upsertTable(customerSpace.toString(), activeTableNames.get(0), role, inactive);
            }
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
