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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.manage.Progress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.RefreshJobExecutor;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.core.source.RawSource;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.Source;

@Component("progressOrchestrator")
public class ProgressOrchestrator {

    @Autowired
    private ApplicationContext ac;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Autowired
    private SourceService sourceService;

    @Value("${propdata.job.schedule.dryrun:true}")
    Boolean dryrun;

    private Log log = LogFactory.getLog(this.getClass());
    private Map<RawSource, ArchiveService> archiveServiceMap = new HashMap<>();
    private Map<DerivedSource, RefreshService> refreshServiceMap = new HashMap<>();
    private static final int jobExpirationHours = 48; // expire a job after 48 hour
    private static final long jobExpirationMilliSeconds = TimeUnit.HOURS.toMillis(jobExpirationHours);
    private Map<String, RefreshJobExecutor> executorMap = new HashMap<>();
    private ExecutorService executorService;

    @PostConstruct
    private void constructMaps() {
        for (Source source: sourceService.getSources()) {
            Object service = ac.getBean(source.getRefreshServiceBean());
            if (service != null) {
                if (source instanceof RawSource) {
                    archiveServiceMap.put((RawSource) source, (ArchiveService) service);
                    executorMap.put(source.getSourceName(),
                            new ArchiveExecutor((ArchiveService) service));
                } else if (source instanceof DerivedSource) {
                    refreshServiceMap.put((DerivedSource) source, (RefreshService) service);
                    executorMap.put(source.getSourceName(),
                            new RefreshExecutor((RefreshService) service));
                }
            }
        }

    }

    public synchronized void executeRefresh() {
        executorService = Executors.newFixedThreadPool(executorMap.size());

        for (RawSource source: archiveServiceMap.keySet()) {
            try {
                if (zkConfigurationService.refreshJobEnabled(source) && (!dryrun)) {
                    submitProgress(findArchiveProgressToProceed(source));
                }
            } catch (Exception e) {
                log.error("Failed to find progress to proceed for " + source.getSourceName(), e);
            }
        }

        for (DerivedSource source: refreshServiceMap.keySet()) {
            try {
                if (zkConfigurationService.refreshJobEnabled(source) && (!dryrun)) {
                    submitProgress(findRefreshProgressToProceed(source));
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

    @SuppressWarnings("unchecked")
    ArchiveProgress findArchiveProgressToProceed(RawSource source) {
        ArchiveService archiveService = archiveServiceMap.get(source);
        return (ArchiveProgress)
                findProgressToProceedForSource((SourceRefreshServiceBase<Progress>) archiveService);
    }

    @SuppressWarnings("unchecked")
    RefreshProgress findRefreshProgressToProceed(DerivedSource source) {
        RefreshService refreshService = refreshServiceMap.get(source);
        return (RefreshProgress) findProgressToProceedForSource((SourceRefreshServiceBase<Progress>) refreshService);
    }

    Progress findProgressToProceedForSource(SourceRefreshServiceBase<Progress> service) {
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
            case UPLOADED:
            case FAILED:
                return true;
            default:
                return false;
        }
    }

    void setServiceMaps(Map<RawSource, ArchiveService> archiveServiceMap,
                        Map<DerivedSource, RefreshService> pivotServiceMap) {
        this.archiveServiceMap = archiveServiceMap;
        this.refreshServiceMap = pivotServiceMap;
    }

}
