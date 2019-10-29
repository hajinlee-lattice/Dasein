package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("buildRawActivityStreamWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildRawActivityStreamWrapper
        extends BaseTransformationWrapper<ProcessActivityStreamStepConfiguration, BuildRawActivityStream> {

    @Inject
    private BuildRawActivityStream buildRawActivityStream;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(BuildRawActivityStream.BEAN_NAME);
    }

    @Override
    protected BuildRawActivityStream getWrapperStep() {
        return buildRawActivityStream;
    }
}
