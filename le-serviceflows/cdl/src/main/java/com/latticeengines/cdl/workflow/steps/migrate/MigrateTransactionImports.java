package com.latticeengines.cdl.workflow.steps.migrate;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateTransactionImportStepConfiguration;

@Component(MigrateTransactionImports.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateTransactionImports extends BaseMigrateImports<MigrateTransactionImportStepConfiguration> {

    static final String BEAN_NAME = "migrateTransactionImports";
}
