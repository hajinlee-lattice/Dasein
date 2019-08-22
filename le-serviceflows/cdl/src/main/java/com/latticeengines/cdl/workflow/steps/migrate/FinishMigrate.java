package com.latticeengines.cdl.workflow.steps.migrate;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.EntityMatchMigrateStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishMigrate")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishMigrate extends BaseWorkflowStep<EntityMatchMigrateStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(FinishMigrate.class);

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    //TODO: remove this step after log checks out.
    @Override
    public void execute() {
        ImportMigrateTracking migrateTracking =
                migrateTrackingProxy.getMigrateTracking(configuration.getCustomerSpace().toString(),
                        configuration.getMigrateTrackingPid());
        log.info(String.format("EntityMatchImportMigration completed, detail info: %s",
                JsonUtils.serialize(migrateTracking.getReport())));

    }
}
