package com.latticeengines.cdl.workflow.steps.rating;


import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

// this dummy step is for choreographer to process iteration status
@Component("postIterationInitialization")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PostIterationInitialization extends BaseWorkflowStep<BaseStepConfiguration> {

    @Override
    public void execute() {}

}
