package com.latticeengines.cdl.workflow.steps.migrate;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateAccountImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("migrateAccountImportsWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateAccountImportsWrapper
        extends BaseTransformationWrapper<MigrateAccountImportStepConfiguration, MigrateAccountImports> {

    @Inject
    private MigrateAccountImports migrateAccountImports;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MigrateAccountImports.BEAN_NAME);
    }

    @Override
    protected MigrateAccountImports getWrapperStep() {
        return migrateAccountImports;
    }
}
