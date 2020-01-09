package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("matchRawStreamWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchRawStreamWrapper
        extends BaseTransformationWrapper<ProcessActivityStreamStepConfiguration, MatchRawStream> {

    @Inject
    private MatchRawStream matchRawStream;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MatchRawStream.BEAN_NAME);
    }

    @Override
    protected MatchRawStream getWrapperStep() {
        return matchRawStream;
    }
}
