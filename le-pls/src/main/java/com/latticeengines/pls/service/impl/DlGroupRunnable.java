package com.latticeengines.pls.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.JobStatus;
import com.latticeengines.domain.exposed.dataloader.LaunchJobsResult;
import com.latticeengines.pls.service.DlCallback;
import com.latticeengines.remote.exposed.service.DataLoaderService;

public class DlGroupRunnable implements Runnable {

    private static final Log log = LogFactory.getLog(DlGroupRunnable.class);
    private static final long sleepTime = 5000L;

    private String dlUrl;
    private long launchId;
    private DataLoaderService dataLoaderService;
    private DlCallback onProgress;
    private DlCallback onCompleted;

    public DlGroupRunnable(String dlUrl, long launchId, DataLoaderService dataLoaderService) {
        this.dlUrl = dlUrl;
        this.launchId = launchId;
        this.dataLoaderService = dataLoaderService;
    }

    public void setProgressCallback(DlCallback onProgress) {
        this.onProgress = onProgress;
    }

    public void setCompletedCallback(DlCallback onCompleted) {
        this.onCompleted = onCompleted;
    }

    @Override
    public void run() {
        try
        {
            log.info(String.format("Start to listen the status. DL url: %s, launch id: %d.", dlUrl, launchId));

            LaunchJobsResult result;
            while (true) {
                result = dataLoaderService.getLaunchJobs(launchId, dlUrl);
                if (result.getLaunchStatus() == JobStatus.SUCCESS || 
                        result.getLaunchStatus() == JobStatus.FAIL) {
                    break;
                } else {
                    if (onProgress != null) {
                        onProgress.callback(result);
                    }
                }

                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    // Do nothing if sleep interrupted
                }
            }

            if (onCompleted != null) {
                onCompleted.callback(result);
            }

            log.info(String.format("End to listen the status. DL url: %s, launch id: %d.", dlUrl, launchId));
        } catch (Exception ex) {
            if (onCompleted != null) {
                onCompleted.callback(ex);
            }

            log.error(String.format("Get launch status encountered an exception. DL url: %s, launch id: %d.", dlUrl, launchId), ex);
        }
    }

}
