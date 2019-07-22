package com.latticeengines.cdl.workflow.steps.migrate;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateAccountImportStepConfiguration;

@Component(MigrateAccountImports.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateAccountImports extends BaseMigrateImports<MigrateAccountImportStepConfiguration> {

    static final String BEAN_NAME = "migrateAccountImports";

}
