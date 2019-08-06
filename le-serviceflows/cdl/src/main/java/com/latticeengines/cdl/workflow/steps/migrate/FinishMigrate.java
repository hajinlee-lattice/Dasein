package com.latticeengines.cdl.workflow.steps.migrate;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.EntityMatchMigrateStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishMigrate")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishMigrate extends BaseWorkflowStep<EntityMatchMigrateStepConfiguration> {

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        migrateTrackingProxy.updateStatus(configuration.getCustomerSpace().toString(),
                configuration.getMigrateTrackingPid(), ImportMigrateTracking.Status.COMPLETED);
        //update MigrationTrack record.
//        metadataProxy.updateImportTracking(configuration.getCustomerSpace().toString(),
//                configuration.getMigrateTrackingPid());
    }
}
