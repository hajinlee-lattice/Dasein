package com.latticeengines.cdl.workflow.steps.migrate;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateContactImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("migrateContactImportsWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateContactImportWrapper
        extends BaseTransformationWrapper<MigrateContactImportStepConfiguration, MigrateContactImports> {

    @Inject
    private MigrateContactImports migrateContactImports;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MigrateContactImports.BEAN_NAME);
    }

    @Override
    protected MigrateContactImports getWrapperStep() {
        return migrateContactImports;
    }
}
