package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculateStatsStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("finishProfile")
public class FinishProfile extends BaseWorkflowStep<CalculateStatsStepConfiguration> {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private String customerSpace;
    private DataCollection.Version activeVersion;
    private DataCollection.Version inactiveVersion;

    @Override
    public void execute() {
        log.info("Inside FinishProfile execute()");
        customerSpace = configuration.getCustomerSpace().toString();
        activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);
        Arrays.stream(BusinessEntity.values()).forEach(this::switchEntityVersion);
    }

    private void switchEntityVersion(BusinessEntity entity) {
        TableRoleInCollection batchStore = entity.getBatchStore();
        if (batchStore != null) {
            cloneTableRole(batchStore);
        }
        copyAggregatedTransaction();
    }

    private void cloneTableRole(TableRoleInCollection role) {
        Table activeBatchStore = dataCollectionProxy.getTable(customerSpace, role, activeVersion);
        if (activeBatchStore != null) {
            Table clone = metadataProxy.cloneTable(customerSpace, activeBatchStore.getName());
            dataCollectionProxy.upsertTable(customerSpace, clone.getName(), role, inactiveVersion);
            log.info("Clone and upsert " + role + " from version " + activeVersion + " to " + inactiveVersion);
        }
    }

    private void copyAggregatedTransaction() {
        TableRoleInCollection role = TableRoleInCollection.AggregatedTransaction;
        Table activeTxn = dataCollectionProxy.getTable(customerSpace, role, activeVersion);
        if (activeTxn != null) {
            Table inactiveTxn = dataCollectionProxy.getTable(customerSpace, role, inactiveVersion);
            if (inactiveTxn == null) {
                log.info(role + " table exists in version " + activeVersion + " but not in version " + inactiveVersion
                        + ", registering it in " + inactiveVersion + " as well.");
                dataCollectionProxy.upsertTable(customerSpace, activeTxn.getName(), role, inactiveVersion);
            } else {
                log.info(role + " does not exists in either version. Skip copying.");
            }
        }
    }

}
