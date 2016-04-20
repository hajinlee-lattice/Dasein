package com.latticeengines.workflow.core;

import java.util.Set;

import javax.annotation.PostConstruct;

import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.FailureReportingListener;
import com.latticeengines.workflow.listener.LEJobListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.listener.LogJobListener;

@Configuration
public class WorkflowTranslator {

    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    private BeanValidationService beanValidationService = new BeanValidationServiceImpl();

    @PostConstruct
    public void init() {
        jobBuilderFactory = new LEJobBuilderFactory(jobRepository, new LogJobListener(), new FailureReportingListener(workflowJobEntityMgr));
    }

    public Job buildWorkflow(String name, Workflow workflow) throws Exception {
        if (workflow.isDryRun()) {
            for (AbstractStep<?> step : workflow.getSteps()) {
                step.setDryRun(true);
            }
        }

        SimpleJobBuilder simpleJobBuilder = jobBuilderFactory.get(name).start(step(workflow.getSteps().get(0)));
        if (workflow.getSteps().size() > 1) {
            for (int i = 1; i < workflow.getSteps().size(); i++) {
                simpleJobBuilder = simpleJobBuilder.next(step(workflow.getSteps().get(i)));
            }
        }

        for (LEJobListener listener : workflow.getListeners()) {
            simpleJobBuilder = simpleJobBuilder.listener(listener);
        }
        return simpleJobBuilder.build();
    }

    protected Step step(AbstractStep<?> step) throws Exception {
        return stepBuilderFactory.get(step.name()) //
                .tasklet(tasklet(step)) //
                .allowStartIfComplete(step.isRunAgainWhenComplete()) //
                .build();
    }

    protected Tasklet tasklet(final AbstractStep<?> step) {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext context) {

                JobParameters jobParameters = context.getStepContext().getStepExecution().getJobParameters();
                step.setJobParameters(jobParameters);

                ExecutionContext executionContext = context.getStepContext().getStepExecution().getJobExecution()
                        .getExecutionContext();
                step.setExecutionContext(executionContext);

                if (!step.isDryRun()) {
                    boolean configurationWasSet = step.setup();
                    if (configurationWasSet) {
                        validateConfiguration(step);
                    }

                    step.execute();
                    step.onExecutionCompleted();
                }

                return RepeatStatus.FINISHED;
            }

            private void validateConfiguration(final AbstractStep<?> step) {
                Set<AnnotationValidationError> validationErrors = beanValidationService.validate(step
                        .getConfiguration());
                if (validationErrors.size() > 0) {
                    StringBuilder validationErrorStringBuilder = new StringBuilder();
                    for (AnnotationValidationError annotationValidationError : validationErrors) {
                        validationErrorStringBuilder.append(annotationValidationError.getFieldName() + ":"
                                + annotationValidationError.getAnnotationName() + "\n");
                    }

                    throw new LedpException(LedpCode.LEDP_28008, new String[] { step.getConfiguration().toString(),
                            validationErrorStringBuilder.toString() });
                }
            }
        };
    }

}
