package com.latticeengines.workflow.core;

import javax.annotation.PostConstruct;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.latticeengines.workflow.build.AbstractStep;
import com.latticeengines.workflow.listener.LogJobListener;

@Configuration
public class WorkflowTranslator {

    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @PostConstruct
    public void init() {
        jobBuilderFactory = new LEJobBuilderFactory(jobRepository, new LogJobListener());
    }

    public Job buildWorkflow(String name, Workflow workflow) throws Exception {
        if (workflow.isDryRun()) {
            for (AbstractStep step : workflow.getSteps()) {
                step.setDryRun(true);
            }
        }

        SimpleJobBuilder simpleJobBuilder = jobBuilderFactory.get(name).start(step(workflow.getSteps().get(0)));
        if (workflow.getSteps().size() > 1) {
            for (int i = 1; i < workflow.getSteps().size(); i++) {
                simpleJobBuilder = simpleJobBuilder.next(step(workflow.getSteps().get(i)));
            }
        }
        return simpleJobBuilder.build();
    }

    protected Step step(AbstractStep step) throws Exception {
        return stepBuilderFactory.get(step.name()) //
        .tasklet(tasklet(step)) //
        .allowStartIfComplete(step.isRunAgainWhenComplete()) //
        .build();
    }

    protected Tasklet tasklet(final AbstractStep step) {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext context) {
                // TODO Configuration for each job and step JobParameters jobParameters = context.getStepContext().getStepExecution().getJobParameters();

                if (!step.isDryRun()) {
                    step.execute();
                }

                return RepeatStatus.FINISHED;
            }
        };
    }

}
