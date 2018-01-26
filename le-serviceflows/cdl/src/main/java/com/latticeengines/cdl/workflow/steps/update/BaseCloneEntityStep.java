package com.latticeengines.cdl.workflow.steps.update;

import java.util.List;

import javax.inject.Inject;

import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public abstract class BaseCloneEntityStep<T extends BaseProcessEntityStepConfiguration> extends BaseWorkflowStep<T> {

    @Inject
    private CloneTableService cloneTableService;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        cloneTableService.setActiveVersion(active);
        cloneTableService.setCustomerSpace(customerSpace);
        for (TableRoleInCollection role : tablesToClone()) {
            cloneTableService.cloneToInactiveTable(role);
        }
    }

    protected abstract List<TableRoleInCollection> tablesToClone();

}
