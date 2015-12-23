package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.service.RefreshJobExecutor;
import com.latticeengines.propdata.collection.service.ZkConfigurationService;
import com.latticeengines.propdata.collection.source.impl.CollectionSource;
import com.latticeengines.propdata.collection.source.impl.PivotedSource;

@Component("progressOrchestrator")
public class ProgressOrchestrator {

    @Autowired
    private ApplicationContext ac;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    private Log log = LogFactory.getLog(this.getClass());
    private Map<CollectionSource, ArchiveService> archiveServiceMap = new HashMap<>();
    private Map<PivotedSource, PivotService> pivotServiceMap = new HashMap<>();
    private static final int jobExpirationHours = 48; // expire a job after 48 hour
    private static final long jobExpirationMilliSeconds = TimeUnit.HOURS.toMillis(jobExpirationHours);
    private Map<String, RefreshJobExecutor> executorMap = new HashMap<>();
    private ExecutorService executorService;

    @PostConstruct
    private void constructMaps() {
        for (CollectionSource source: CollectionSource.values()) {
            if (!CollectionSource.TEST_COLLECTION.equals(source)) {
                ArchiveService service = (ArchiveService) ac.getBean(source.getRefreshServiceBean());
                if (service != null) {
                    archiveServiceMap.put(source, service);
                    executorMap.put(source.getSourceName(), new CollectionRefreshExecutor(service));
                }
            }
        }

        for (PivotedSource source: PivotedSource.values()) {
            if (!PivotedSource.TEST_PIVOTED.equals(source)) {
                PivotService service = (PivotService) ac.getBean(source.getRefreshServiceBean());

                if (service != null) {
                    pivotServiceMap.put(source, service);
                    executorMap.put(source.getSourceName(), new PivotedRefreshExecutor(service));
                }
            }
        }
    }

    public synchronized void executeRefresh() {
        executorService = Executors.newFixedThreadPool(executorMap.size());

        for (CollectionSource source: archiveServiceMap.keySet()) {
            try {
                if (zkConfigurationService.refreshJobEnabled(source)) {
                    submitProgress(findArchiveProgressToProceed(source));
                }
            } catch (Exception e) {
                log.error("Failed to find progress to proceed for " + source.getSourceName(), e);
            }
        }

        for (PivotedSource source: pivotServiceMap.keySet()) {
            try {
                if (zkConfigurationService.refreshJobEnabled(source)) {
                    submitProgress(findPivotProgressToProceed(source));
                }
            } catch (Exception e) {
                log.error("Failed to find progress to proceed for " + source.getSourceName(), e);
            }
        }
    }

    private void submitProgress(final Progress progress) {
        if (progress == null) { return; }
        final RefreshJobExecutor executor = executorMap.get(progress.getSourceName());
        if (executor != null) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    executor.proceedProgress(progress);
                }
            });
        }
    }

    @SuppressWarnings("unused")
    private void submitProgressTest(String sourceName) {
        final RefreshJobExecutor executor = executorMap.get(sourceName);
        if (executor != null) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    executor.proceedProgress(null);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    ArchiveProgress findArchiveProgressToProceed(CollectionSource source) {
        ArchiveService archiveService = archiveServiceMap.get(source);
        return (ArchiveProgress) findProgressToProceedForSource((AbstractSourceRefreshService<Progress>) archiveService);
    }

    @SuppressWarnings("unchecked")
    PivotProgress findPivotProgressToProceed(PivotedSource source) {
        PivotService pivotService = pivotServiceMap.get(source);
        PivotProgress progressToProceed =
                (PivotProgress) findProgressToProceedForSource((AbstractSourceRefreshService<Progress>) pivotService);
        if (progressToProceed == null) {
            return pivotService.startNewProgressIfOutDated(this.getClass().getSimpleName());
        } else {
            return progressToProceed;
        }
    }

    Progress findProgressToProceedForSource(AbstractSourceRefreshService<Progress> service) {
        Progress runningProgress = service.findRunningJob();
        if (runningProgress != null) { // an old job is running
            if (shouldStartNextStep(runningProgress.getStatus())) {
                return runningProgress;
            } else {
                Date expireDate = new Date(System.currentTimeMillis() - jobExpirationMilliSeconds);
                Date lastUpdated = runningProgress.getLatestStatusUpdate();
                if (lastUpdated.before(expireDate)) {
                    log.fatal(String.format(
                            "This progress has been hanging for more than %d hours: %s",
                            jobExpirationHours, runningProgress));
                    runningProgress.setNumRetries(99);
                    service.updateStatusToFailed(runningProgress, "Timeout", null);
                }
                return null;
            }
        }
        return service.findJobToRetry();
    }

    private boolean shouldStartNextStep(ProgressStatus status) {
        switch (status) {
            case NEW:
            case DOWNLOADED:
            case TRANSFORMED:
            case PIVOTED:
            case UPLOADED:
            case FAILED:
                return true;
            default:
                return false;
        }
    }

    void setServiceMaps(Map<CollectionSource, ArchiveService> archiveServiceMap,
                        Map<PivotedSource, PivotService> pivotServiceMap) {
        this.archiveServiceMap = archiveServiceMap;
        this.pivotServiceMap = pivotServiceMap;
    }

}
