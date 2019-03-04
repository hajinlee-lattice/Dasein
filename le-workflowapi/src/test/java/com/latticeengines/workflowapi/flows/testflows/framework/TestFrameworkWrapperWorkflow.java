package com.latticeengines.workflowapi.flows.testflows.framework;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

// Workflow from wrapping the step or workflow under test in a super-workflow containing a preprocessing
// that setups test infrastructure and a postprocessing step for tearing down test insfrastructure and
// performing validation.
@Component("testFrameworkWrapperWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TestFrameworkWrapperWorkflow extends AbstractWorkflow<TestFrameworkWrapperWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(TestFrameworkWrapperWorkflow.class);

    @Inject
    private ApplicationContext appCtx;

    @Override
    public Workflow defineWorkflow(TestFrameworkWrapperWorkflowConfiguration config) {
        TestBasePreprocessingStep<?> preStep = (TestBasePreprocessingStep<?>) appCtx.getBean(config.getPreStepBeanName());
        TestBasePostprocessingStep<?> postStep = (TestBasePostprocessingStep<?>) appCtx.getBean(config.getPostStepBeanName());

        if (config.isTestingSingleStep()) {
            BaseWorkflowStep<?> testStep = (BaseWorkflowStep<?>) appCtx.getBean(config.getTestStepBeanName());
            return new WorkflowBuilder(name(), config) //
                    .next(preStep) //
                    .next(testStep) //
                    .next(postStep) //
                    .build();

        } else {
            AbstractWorkflow<?> testWorkflow = (AbstractWorkflow<?>) appCtx.getBean(config.getTestWorkflowName());
            return new WorkflowBuilder(name(), config) //
                    .next(preStep) //
                    .next(testWorkflow) //
                    .next(postStep) //
                    .build();
        }
    }
}
