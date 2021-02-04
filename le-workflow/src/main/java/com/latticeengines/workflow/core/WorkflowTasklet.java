package com.latticeengines.workflow.core;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.TRACING_CONTEXT;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.lang.Nullable;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.InjectableFailure;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.Choreographer;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

class WorkflowTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(WorkflowTasklet.class);

    private final AbstractStep<? extends BaseStepConfiguration> step;
    private final Choreographer choreographer;
    private final int seq;
    private final InjectableFailure injectableFailure;
    private final Map<String, String> initialContext;
    private final Map<String, String> tracingContext;

    private BeanValidationService beanValidationService = new BeanValidationServiceImpl();

    WorkflowTasklet(AbstractStep<? extends BaseStepConfiguration> step, //
                    Choreographer choreographer, int seq, InjectableFailure injectableFailure, //
                    Map<String, String> initialContext, Map<String, String> tracingContext) {
        this.step = step;
        this.choreographer = choreographer;
        this.seq = seq;
        this.injectableFailure = injectableFailure;
        this.initialContext = initialContext;
        this.tracingContext = tracingContext;
    }

    @Override
    public RepeatStatus execute(@Nullable StepContribution contribution, ChunkContext context) {
        log.info("step {} has namespace {}", step.name(), step.getNamespace());
        StepExecution stepExecution = context.getStepContext().getStepExecution();
        JobParameters jobParameters = stepExecution.getJobParameters();
        step.setJobParameters(jobParameters);

        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        if (seq == 0 && MapUtils.isNotEmpty(initialContext)) {
            log.info("Initializing context: " + JsonUtils.serialize(initialContext));
            initialContext.forEach(executionContext::putString);
        }
        step.setExecutionContext(executionContext);

        step.setSeq(seq);
        step.setInjectedFailure(injectableFailure);

        if (step.isDryRun()) {
            return RepeatStatus.FINISHED;
        }

        boolean configurationWasSet = step.setup();
        boolean shouldSkip = choreographer.skipStep(step, seq);
        if (shouldSkip) {
            step.skipStep();
            stepExecution.setExitStatus(ExitStatus.NOOP);
            return RepeatStatus.FINISHED;
        }
        if (!configurationWasSet && step.skipOnMissingConfiguration()) {
            log.info("configuration for step {} is not set. skipping", step.name());
            try {
                step.skipStep();
            } catch (Exception e) {
                String msg = String.format("failed to skip step %s on missing configuration", step.name());
                log.warn(msg, e);
            }
            stepExecution.setExitStatus(ExitStatus.NOOP);
            return RepeatStatus.FINISHED;
        }

        Tracer tracer = GlobalTracer.get();
        Span stepSpan = null;
        try (Scope scope = startStepSpan()) {
            stepSpan = tracer.activeSpan();
            step.putObjectInContext(TRACING_CONTEXT, TracingUtils.getActiveTracingContext());
            stepSpan.log("config initialized");
            step.onConfigurationInitialized();
            if (configurationWasSet) {
                stepSpan.log("start validating config");
                validateConfiguration(step);
            }
            step.setJobId(context.getStepContext().getStepExecution().getJobExecution().getId());
            step.throwFailureIfInjected(InjectableFailure.BeforeExecute);
            stepSpan.log("start execution");
            step.execute();
            stepSpan.log("finish execution");
            step.onExecutionCompleted();
            step.throwFailureIfInjected(InjectableFailure.AfterExecute);
        } catch (Exception e) {
            TracingUtils.logError(stepSpan, e,
                    String.format("Failed at step #%d - %s", step.getSeq(), step.name()));
            // rethrow
            throw e;
        } finally {
            TracingUtils.finish(stepSpan);
        }

        return RepeatStatus.FINISHED;
    }

    private void validateConfiguration(final AbstractStep<?> step) {
        Set<AnnotationValidationError> validationErrors = beanValidationService
                .validate(step.getConfiguration());
        if (validationErrors.size() > 0) {
            StringBuilder validationErrorStringBuilder = new StringBuilder();
            for (AnnotationValidationError annotationValidationError : validationErrors) {
                validationErrorStringBuilder //
                        .append(annotationValidationError.getFieldName()) //
                        .append(":") //
                        .append(annotationValidationError.getAnnotationName()) //
                        .append("\n");
            }

            throw new LedpException(LedpCode.LEDP_28008, new String[] { step.getConfiguration().toString(),
                    validationErrorStringBuilder.toString() });
        }
    }

    private Scope startStepSpan() {
        Tracer tracer = GlobalTracer.get();
        SpanContext workflowCtx = TracingUtils.getSpanContext(tracingContext);
        Span span = tracer.buildSpan(step.name()) //
                .withTag(TracingTags.Workflow.NAMESPACE, step.getNamespace()) //
                .withTag(TracingTags.Workflow.STEP_SEQ, seq) //
                .asChildOf(workflowCtx) //
                .start();
        return tracer.activateSpan(span);
    }
}
