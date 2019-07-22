package com.latticeengines.cdl.workflow.steps.migrate;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateTransactionImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("migrateTransactionImportsWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateTransactionImportWrapper
        extends BaseTransformationWrapper<MigrateTransactionImportStepConfiguration, MigrateTransactionImports> {

    @Inject
    private MigrateTransactionImports migrateTransactionImports;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MigrateTransactionImports.BEAN_NAME);
    }

    @Override
    protected MigrateTransactionImports getWrapperStep() {
        return migrateTransactionImports;
    }
}
