package com.latticeengines.workflowapi.yarn.runtime;

import java.util.Collection;
import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.ApplicationContext;

import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.util.WorkflowConfigurationUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

@StepScope
public class WorkflowProcessor extends SingleContainerYarnProcessor<WorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowProcessor.class);

    @Inject
    private ApplicationContext appContext;

    @Inject
    private SoftwareLibraryService softwareLibraryService;

    @Inject
    private WorkflowService workflowService;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    @Inject
    private JobCacheService jobCacheService;

    public WorkflowProcessor() {
        super();
        log.info("Constructed WorkflowProcessor.");
    }

    @Override
    public String process(WorkflowConfiguration workflowConfig) {
        if (appId == null) {
            throw new LedpException(LedpCode.LEDP_28022);
        }
        log.info(String.format("Looking up workflow for application id %s", appId));

        if (workflowConfig.getWorkflowName() == null) {
            throw new LedpException(LedpCode.LEDP_28011, new String[] { workflowConfig.toString() });
        }

        String workflowName = workflowConfig.getWorkflowName();
        boolean isRestart = workflowConfig.isRestart();
        // loading java libaries
        log.info("Running WorkflowProcessor with workflowName:{} and config:{}", workflowName,
                workflowConfig.toString());
        Collection<String> swpkgNames = workflowConfig.getSwpkgNames();
        // tracer uses microsecond
        long start = System.currentTimeMillis() * 1000;
        loadLibraries(swpkgNames);
        long end = System.currentTimeMillis() * 1000;

        // can only start tracing after libraries is loaded
        Tracer tracer = GlobalTracer.get();
        SpanContext parentCtx = TracingUtils.getSpanContext(workflowConfig.getTracingContext());
        Span workflowSpan = null;
        try (Scope scope = startWorkflowSpan(parentCtx, workflowName, appId.toString(), start, isRestart)) {
            workflowSpan = tracer.activeSpan();
            traceSoftwareLibrariesLoad(start, end, swpkgNames);

            WorkflowJob workflowJob = workflowJobEntityMgr.findByApplicationId(appId.toString());
            if (workflowJob == null) {
                throw new RuntimeException(String.format("No workflow job found with application id %s", appId));
            }

            workflowSpan.setTag(TracingTags.Workflow.PID, workflowJob.getPid());
            workflowConfig.setTracingContext(TracingUtils.getActiveTracingContext());

            Long submitTime = workflowJob.getStartTimeInMillis();
            if (submitTime != null && submitTime > 0) {
                traceYarnInstantiation(submitTime * 1000, start, parentCtx);
            }

            // start workflow and wait for it to complete
            return startAndAwaitCompletion(workflowConfig, workflowJob, workflowSpan);
        } finally {
            TracingUtils.finish(workflowSpan);
        }
    }

    // the span from workflow submission to java code being executed (mostly yarn
    // queue & instantiate time)
    private void traceYarnInstantiation(long submitTime, long jobStartTime, SpanContext parentContext) {
        if (parentContext == null) {
            return;
        }

        // only trace if there is parent span
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("Yarn Instantiation") //
                .asChildOf(parentContext) //
                .withStartTimestamp(submitTime) //
                .start();
        span.finish(jobStartTime);
    }

    private void traceSoftwareLibrariesLoad(long start, long end, Collection<String> swpkgNames) {
        Tracer tracer = GlobalTracer.get();
        String pkgStr = swpkgNames == null ? "all" : swpkgNames.toString();
        Span loadLibSpan = tracer.buildSpan("loadSoftwareLibraries") //
                .withTag(TracingTags.Workflow.SOFTWARE_LIBRARIES, pkgStr) //
                .asChildOf(tracer.activeSpan()) //
                .withStartTimestamp(start) //
                .start();
        loadLibSpan.finish(end);
    }

    private Scope startWorkflowSpan(SpanContext parentContext, String workflowName, String appId, long startTime,
            boolean isRestart) {
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("Workflow - " + workflowName) //
                .addReference(References.FOLLOWS_FROM, parentContext) //
                .withTag(TracingTags.Workflow.WORKFLOW_NAME, workflowName) //
                .withTag(TracingTags.Workflow.APPLICATION_ID, appId) //
                .withTag(TracingTags.Workflow.IS_RESTART, isRestart) //
                .withStartTimestamp(startTime) //
                .start();
        return tracer.activateSpan(span);
    }

    private void loadLibraries(Collection<String> swpkgNames) {
        if (CollectionUtils.isEmpty(swpkgNames)) {
            log.info("Enriching application context with all sw packages available.");
            appContext = softwareLibraryService.loadSoftwarePackages(SoftwareLibrary.Module.workflowapi.name(),
                    appContext);
        } else {
            log.info("Enriching application context with sw package " + swpkgNames);
            appContext = softwareLibraryService.loadSoftwarePackages(SoftwareLibrary.Module.workflowapi.name(),
                    swpkgNames, appContext);
        }
    }

    private String startAndAwaitCompletion(WorkflowConfiguration workflowConfig, WorkflowJob workflowJob, Span span) {
        try {
            WorkflowExecutionId workflowId;
            workflowService.prepareToSleepForCompletion();
            if (workflowConfig.isRestart()) {
                @SuppressWarnings("unchecked")
                AbstractWorkflow<? extends WorkflowConfiguration> workflow = appContext
                        .getBean(workflowConfig.getWorkflowName(), AbstractWorkflow.class);
                Class<? extends WorkflowConfiguration> workflowConfigClass = workflow.getWorkflowConfigurationType();
                WorkflowConfiguration restartWorkflowConfig = WorkflowConfigurationUtils
                        .getDefaultWorkflowConfiguration(workflowConfigClass);
                restartWorkflowConfig.setWorkflowName(workflowConfig.getWorkflowName());
                restartWorkflowConfig.setWorkflowIdToRestart(workflowConfig.getWorkflowIdToRestart());
                workflowService.registerJob(restartWorkflowConfig, appContext);
                workflowId = workflowService.restart(restartWorkflowConfig.getWorkflowIdToRestart(), workflowJob);
            } else {
                workflowService.registerJob(workflowConfig, appContext);
                workflowId = workflowService.start(workflowConfig, workflowJob);
            }

            if (workflowId != null) {
                span.setTag(TracingTags.Workflow.WORKFLOW_ID, workflowId.getId());
            }
            workflowService.sleepForCompletion(workflowId);
        } catch (Exception exc) {
            workflowJob.setStatus(JobStatus.FAILED.name());
            workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
            if (workflowJob.getWorkflowId() != null) {
                jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowJob.getWorkflowId()));
            }
            TracingUtils.logError(span, exc, "Fail when start and wait workflow completion");

            ErrorDetails details;
            if (exc instanceof LedpException) {
                LedpException casted = (LedpException) exc;
                details = casted.getErrorDetails();
            } else {
                details = new ErrorDetails(LedpCode.LEDP_00002, exc.getMessage(), ExceptionUtils.getStackTrace(exc));
            }
            workflowJob.setErrorDetails(details);
            workflowJobEntityMgr.updateErrorDetails(workflowJob);

            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowJob.getPid());
            jobUpdate.setLastUpdateTime(System.currentTimeMillis());
            workflowJobUpdateEntityMgr.updateLastUpdateTime(jobUpdate);
            throw new RuntimeException(exc);
        }
        return null;
    }
}
