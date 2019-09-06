package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BuildCatalogStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("buildCatalogWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildCatalogWrapper extends BaseTransformationWrapper<BuildCatalogStepConfiguration, BuildCatalog> {

    @Inject
    private BuildCatalog buildCatalog;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(BuildCatalog.BEAN_NAME);
    }

    @Override
    protected BuildCatalog getWrapperStep() {
        return buildCatalog;
    }
}
