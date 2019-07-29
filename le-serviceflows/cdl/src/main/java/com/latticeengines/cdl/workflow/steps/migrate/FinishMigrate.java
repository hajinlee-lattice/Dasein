package com.latticeengines.cdl.workflow.steps.migrate;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.MigrateTracking;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.EntityMatchMigrateStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishMigrate")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishMigrate extends BaseWorkflowStep<EntityMatchMigrateStepConfiguration> {

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    @Override
    public void execute() {
        migrateTrackingProxy.updateStatus(configuration.getCustomerSpace().toString(),
                configuration.getMigrateTrackingPid(), MigrateTracking.Status.COMPLETED);
    }
}
