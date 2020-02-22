package com.latticeengines.cdl.workflow.steps.update;

import java.util.List;

import javax.inject.Inject;

import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.cdl.workflow.steps.EntityAwareWorkflowStep;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;

public abstract class BaseCloneEntityStep<T extends BaseProcessEntityStepConfiguration>
        extends EntityAwareWorkflowStep<T> {

    @Inject
    private CloneTableService cloneTableService;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        cloneTableService.setActiveVersion(active);
        cloneTableService.setCustomerSpace(customerSpace);
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null) {
            cloneTableService.setRedshiftPartition(dcStatus.getRedshiftPartition());
        }
        for (TableRoleInCollection role : tablesToClone()) {
            cloneTableService.cloneToInactiveTable(role);
        }
        for (TableRoleInCollection role : tablesToLink()) {
            cloneTableService.linkInactiveTable(role);
        }
    }

    protected abstract List<TableRoleInCollection> tablesToClone();

    protected abstract List<TableRoleInCollection> tablesToLink();

}
