package com.latticeengines.propdata.engine.transformation;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.service.ServiceFlowsZkConfigService;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.service.TransformationExecutor;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.propdata.engine.transformation.service.impl.TransformationExecutorImpl;
import com.latticeengines.propdata.engine.transformation.service.impl.TransformationServiceBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("transformationProgressOrchestrator")
public class TransformationProgressOrchestrator {

    @Autowired
    private ServiceFlowsZkConfigService serviceFlowsZkConfigService;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private List<TransformationService> transformationServiceList;

    @Value("${propdata.job.schedule.dryrun:true}")
    private Boolean dryrun;

    private Log log = LogFactory.getLog(this.getClass());
    private Map<Source, TransformationService> transformationServiceMap = new HashMap<>();
    private static final int jobExpirationHours = 48; // expire after 48 hour
    private static final long jobExpirationMilliSeconds = TimeUnit.HOURS.toMillis(jobExpirationHours);
    private Map<String, TransformationExecutor> executorMap = new HashMap<>();
    private ExecutorService executorService;

    @PostConstruct
    private void constructMaps() {
        for (Source source : sourceService.getSources()) {
            for (TransformationService transformationService : transformationServiceList) {
                if (source.equals(transformationService.getSource())) {
                    transformationServiceMap.put(source, transformationService);
                    WorkflowProxy proxy = new WorkflowProxy();
                    executorMap.put(source.getSourceName(),
                            new TransformationExecutorImpl(transformationService, proxy, hdfsPathBuilder));
                }
            }
        }
        executorService = Executors.newFixedThreadPool(executorMap.size());
    }

    public synchronized void executeRefresh() {
        for (Source source : sourceService.getSources()) {
            if (serviceFlowsZkConfigService.refreshJobEnabled(source) && (!dryrun)) {
                try {
                    scheduleProceedProgressWorkflow(findTransformationProgressToProceed(source));
                } catch (Exception e) {
                    log.error("Failed to find progress to proceed for " + source.getSourceName(), e);
                }
            }
        }
    }

    private void scheduleProceedProgressWorkflow(final TransformationProgress progress) {
        if (progress == null) {
            return;
        }

        final TransformationExecutor executor = executorMap.get(progress.getSourceName());
        if (executor != null) {
            executor.proceedProgress(progress);
        }
    }

    @SuppressWarnings("unchecked")
    TransformationProgress findTransformationProgressToProceed(Source source) {
        TransformationService transformationService = transformationServiceMap.get(source);
        return (TransformationProgress) findProgressToProceedForTransformation(
                (TransformationServiceBase) transformationService);
    }

    private TransformationProgress findProgressToProceedForTransformation(TransformationServiceBase ingestionService) {
        TransformationProgress runningProgress = ingestionService.findRunningJob();
        if (runningProgress != null) { // an old job is running
            if (shouldStartNextStep(runningProgress.getStatus())) {
                return runningProgress;
            } else {
                Date expireDate = new Date(System.currentTimeMillis() - jobExpirationMilliSeconds);
                Date lastUpdated = runningProgress.getLatestStatusUpdate();
                if (lastUpdated.before(expireDate)) {
                    log.fatal(String.format("This progress has been hanging for more than %d hours: %s",
                            jobExpirationHours, runningProgress));
                    runningProgress.setNumRetries(99);
                    ingestionService.updateStatusToFailed(runningProgress, "Timeout", null);
                }
                return null;
            }
        }
        return ingestionService.findJobToRetry();
    }

    private boolean shouldStartNextStep(TransformationProgressStatus status) {
        switch (status) {
        case NEW:
        case FINISHED:
        case FAILED:
            return true;
        default:
            return false;
        }
    }

    public TransformationService getTransformationService(Source source) {
        return transformationServiceMap.get(source);
    }

}
