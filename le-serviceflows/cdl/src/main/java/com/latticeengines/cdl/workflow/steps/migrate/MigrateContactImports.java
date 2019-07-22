package com.latticeengines.cdl.workflow.steps.migrate;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateContactImportStepConfiguration;

@Component(MigrateContactImports.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateContactImports extends BaseMigrateImports<MigrateContactImportStepConfiguration> {

    static final String BEAN_NAME = "migrateContactImports";

}
