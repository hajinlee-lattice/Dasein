package com.latticeengines.workflow.core;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.FailingStep;
import com.latticeengines.domain.exposed.workflow.InjectableFailure;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.Choreographer;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.FailureReportingListener;
import com.latticeengines.workflow.listener.FinalJobListener;
import com.latticeengines.workflow.listener.LEJobListener;
import com.latticeengines.workflow.listener.LogJobListener;

@Configuration
public class WorkflowTranslator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowTranslator.class);

    private JobBuilderFactory jobBuilderFactory;

    @Inject
    private JobRepository jobRepository;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    private StepBuilderFactory stepBuilderFactory;

    @Resource(name = "resourceLessTransactionManager")
    private PlatformTransactionManager transactionManager;

    @Inject
    private FinalJobListener finalJobListener;

    private BeanValidationService beanValidationService = new BeanValidationServiceImpl();

    @PostConstruct
    public void init() {
        jobBuilderFactory = new LEJobBuilderFactory(jobRepository, finalJobListener, new LogJobListener(),
                new FailureReportingListener(workflowJobEntityMgr));
        stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
    }

    public Job buildWorkflow(String name, Workflow workflow) {
        if (workflow.isDryRun()) {
            for (AbstractStep<?> step : workflow.getSteps()) {
                step.setDryRun(true);
            }
        }

        Choreographer choreographer = workflow.getChoreographer();
        choreographer.linkStepNamespaces(workflow.getStepNamespaces());
        FailingStep failingStep = workflow.getFailingStep();
        log.info("Need to inject failing step " + JsonUtils.serialize(failingStep));

        Map<String, String> initialContext = workflow.getInitialContext();
        Map<String, String> tracingContext = workflow.getTracingContext();

        Map<String, Integer> stepOccurrences = new HashMap<>();
        if (CollectionUtils.isNotEmpty(workflow.getSteps())) {
            SimpleJobBuilder simpleJobBuilder = null;
            for (int i = 0; i < workflow.getSteps().size(); i++) {
                AbstractStep<? extends BaseStepConfiguration> abstractStep = workflow.getSteps().get(i);
                InjectableFailure failure = null;
                if (failingStep != null) {
                    String stepName = abstractStep.name();
                    if (!stepOccurrences.containsKey(stepName)) {
                        stepOccurrences.put(stepName, 0);
                    }
                    stepOccurrences.put(stepName, stepOccurrences.get(stepName) + 1);
                    boolean shouldFail = shouldFailTheStep(stepName, i, failingStep, stepOccurrences);
                    if (shouldFail) {
                        failure = failingStep.getFailure();
                        if (failure == null) {
                            failure = InjectableFailure.BeforeExecute;
                        }
                        log.info(String.format("Inject %s to [%d] %s", failure, i, stepName));
                    }
                }
                Step step = step(abstractStep, choreographer, i, failure, initialContext, tracingContext);
                if (simpleJobBuilder == null) {
                    simpleJobBuilder = jobBuilderFactory.get(name).start(step);
                } else {
                    simpleJobBuilder = simpleJobBuilder.next(step);
                }
            }
            for (LEJobListener listener : workflow.getListeners()) {
                simpleJobBuilder = simpleJobBuilder.listener(listener);
            }
            return simpleJobBuilder.build();
        } else {
            throw new IllegalArgumentException("Cannot translate empty workflow");
        }
    }

    public Step step(AbstractStep<? extends BaseStepConfiguration> step, Choreographer choreographer, int seq,
            InjectableFailure injectableFailure, Map<String, String> initialContext,
            Map<String, String> tracingContext) {
        return stepBuilderFactory.get(step.name()) //
                .tasklet(tasklet(step, choreographer, seq, injectableFailure, initialContext, tracingContext)) //
                .allowStartIfComplete(step.isRunAgainWhenComplete()) //
                .build();
    }

    private Tasklet tasklet(final AbstractStep<? extends BaseStepConfiguration> step, //
            Choreographer choreographer, int seq, InjectableFailure injectableFailure, //
            Map<String, String> initialContext, Map<String, String> tracingContext) {
        return new WorkflowTasklet(step, choreographer, seq, injectableFailure, initialContext, tracingContext);
    }

    private boolean shouldFailTheStep(String stepName, int seq, FailingStep failingStep,
            Map<String, Integer> stepOccurrences) {
        boolean shouldFail = false;
        Integer failingSeq = failingStep.getSeq();
        if (Integer.valueOf(seq).equals(failingSeq)) {
            shouldFail = true;
        } else {
            String failingStepName = failingStep.getName();
            if (stepName.equals(failingStepName)) {
                Integer failingOccurrence = failingStep.getOccurrence();
                if (failingOccurrence == null) {
                    failingOccurrence = 1;
                }
                int currentOccurrence = stepOccurrences.getOrDefault(failingStepName, 1);
                if (failingOccurrence.equals(currentOccurrence)) {
                    shouldFail = true;
                }
            }
        }
        return shouldFail;
    }

}
