package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileProductHierachyWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileProductHierarchyWrapper extends BaseTransformationWrapper<ProcessProductStepConfiguration, ProfileProductHierarchy> {

    @Inject
    private ProfileProductHierarchy profileProductHierarchy;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileProductHierarchy.BEAN_NAME);
    }

    @Override
    protected ProfileProductHierarchy getWrapperStep() {
        return profileProductHierarchy;
    }

}
