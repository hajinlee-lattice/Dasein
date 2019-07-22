package com.latticeengines.cdl.workflow.steps.migrate;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component("accountTemplateMigrateStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AccountTemplateMigrateStep extends BaseImportTemplateMigrateStep {
}
