package com.latticeengines.cdl.workflow.steps.migrate;


import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("convertActivityStreamToActivityImportWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertActivityStreamToActivityImportWrapper
        extends BaseTransformationWrapper<ProcessActivityStreamStepConfiguration, ConvertActivityStreamToActivityImport> {

    @Inject
    private ConvertActivityStreamToActivityImport convertActivityStreamToActivityImport;

    @Override
    protected ConvertActivityStreamToActivityImport getWrapperStep() {
        return convertActivityStreamToActivityImport;
    }

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ConvertActivityStreamToActivityImport.BEAN_NAME);
    }
}
