package com.latticeengines.cdl.workflow.steps.process;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component(FinishReportGenerationStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishReportGenerationStep extends BaseWorkflowStep<ProcessStepConfiguration> {

    private static Logger log = LoggerFactory.getLogger(FinishReportGenerationStep.class);

    static final String BEAN_NAME = "finishReportGenerationStep";

    @Inject
    private CloneTableService cloneTableService;

    @Override
    public void execute() {
        if (!configuration.isSSVITenant()) {
            return;
        }
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        cloneTableService.setCustomerSpace(customerSpace);
        cloneTableService.setActiveVersion(active);
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null) {
            cloneTableService.setRedshiftPartition(dcStatus.getRedshiftPartition());
        }
        cloneTableService.linkToInactiveTableWithSignature(TableRoleInCollection.ConsolidatedCatalog);
        cloneTableService.linkInactiveTable(TableRoleInCollection.ConsolidateWebVisit);
    }
}
