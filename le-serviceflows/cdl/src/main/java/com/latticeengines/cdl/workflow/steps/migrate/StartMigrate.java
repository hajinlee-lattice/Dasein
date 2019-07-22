package com.latticeengines.cdl.workflow.steps.migrate;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.EntityMatchMigrateStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startMigrate")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StartMigrate extends BaseWorkflowStep<EntityMatchMigrateStepConfiguration> {
    @Override
    public void execute() {

    }
}
