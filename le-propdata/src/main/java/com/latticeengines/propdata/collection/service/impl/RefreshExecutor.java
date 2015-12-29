package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.service.RefreshJobExecutor;
import com.latticeengines.propdata.collection.service.RefreshService;

public class RefreshExecutor implements RefreshJobExecutor {

    private Log log = LogFactory.getLog(this.getClass());

    private final String jobSubmitter;
    private final RefreshService refreshService;
    private final Set<RefreshService> downstreamServices;

    public RefreshExecutor(RefreshService refreshService, Set<RefreshService> downstreamServices) {
        this.refreshService = refreshService;
        this.downstreamServices = downstreamServices;
        this.jobSubmitter = refreshService.getClass().getSimpleName();
    }

    @Override
    public synchronized void proceedProgress(Progress progress) {
        RefreshProgress refreshProgress = (RefreshProgress) progress;
        refreshProgress = retryJob(refreshProgress);
        switch (refreshProgress.getStatus()) {
            case NEW:
                refreshProgress = refreshService.transform(refreshProgress);
                break;
            case TRANSFORMED:
                refreshProgress = refreshService.exportToDB(refreshProgress);
                break;
            case UPLOADED:
                refreshProgress = refreshService.finish(refreshProgress);
                kickOffDownstreamProcesses(refreshProgress);
                break;
            default:
                log.warn(String.format("Illegal starting status %s for progress %s",
                        refreshProgress.getStatus(), refreshProgress.getRootOperationUID()));
        }
        if (refreshProgress.getStatus().equals(ProgressStatus.FAILED)) {
            logJobFailed(refreshProgress);
        } else {
            logJobSucceed(refreshProgress);
        }
    }

    @Override
    public synchronized void print() {
        try {
            String uuid = UUID.randomUUID().toString();
            Random random = new Random();
            System.out.println(refreshService + " - " + uuid + " start.");
            Thread.sleep(random.nextInt(3000));
            System.out.println(refreshService + " - " + uuid + " finished.");

            for (RefreshService service: downstreamServices) {
                uuid = UUID.randomUUID().toString();
                System.out.println(service + " - " + uuid + " start.");
                Thread.sleep(random.nextInt(1000));
                System.out.println(service + " - " + uuid + " finished.");
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private RefreshProgress retryJob(RefreshProgress progress) {
        if (ProgressStatus.FAILED.equals(progress.getStatus())) {
            log.info("Found a job to retry: " + progress);
            ProgressStatus resumeStatus;
            switch (progress.getStatusBeforeFailed()) {
                case NEW:
                case TRANSFORMING:
                    resumeStatus = ProgressStatus.NEW;
                    break;
                case TRANSFORMED:
                case UPLOADING:
                    resumeStatus = ProgressStatus.TRANSFORMED;
                    break;
                default:
                    resumeStatus = ProgressStatus.NEW;
            }
            progress.setStatus(resumeStatus);
        }
        return progress;
    }

    private void kickOffDownstreamProcesses(RefreshProgress progress) {
        String version = refreshService.getVersionString(progress);
        for (RefreshService service : downstreamServices) {
            service.startNewProgress(new Date(), version, jobSubmitter);
        }
    }

    private void logJobSucceed(RefreshProgress progress) {
        log.info("Refreshing " + progress.getSourceName() + " finished for date " +
                progress.getPivotDate() + " RootOperationUID=" + progress.getRootOperationUID());
    }

    private void logJobFailed(RefreshProgress progress) {
        log.error("Refreshing " + progress.getSourceName() + " finished for date " +
                progress.getPivotDate() +" RootOperationUID=" + progress.getRootOperationUID());
    }
}
