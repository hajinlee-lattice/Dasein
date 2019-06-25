package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.DeltaCalculationService;
import com.latticeengines.apps.cdl.workflow.CampaignDeltaCalculationWorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("deltaCalculationService")
public class DeltaCalculationServiceImpl implements DeltaCalculationService {
    private static final Logger log = LoggerFactory.getLogger(DeltaCalculationServiceImpl.class);

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private CampaignDeltaCalculationWorkflowSubmitter campaignDeltaCalculationWorkflowSubmitter;

    public Boolean triggerScheduledCampaigns() {
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllScheduledChannels();

        log.info("Found " + channels.size() + " channels scheduled for launch");

        channels.forEach(c -> {
            String appId = campaignDeltaCalculationWorkflowSubmitter
                    .submit(c.getTenant().getId(), c.getPlay().getName(), c.getId()).toString();
            log.info("Queued a Launch for campaignId " + c.getId() + " : " + appId);
            waitForWorkflowStatus(c.getTenant().getId(), appId);
        });
        // List<Callable<String>> fileExporters = new ArrayList<>();
        // Date fileExportTime = new Date();
        // // fileExporters.add(new CsvFileExporter(yarnConfiguration, config, recAvroHdfsFilePath, fileExportTime));
        // // fileExporters.add(new JsonFileExporter(yarnConfiguration, config, recAvroHdfsFilePath, fileExportTime));
        //
        // ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("playlaunch-export", 2);
        // List<String> exportFiles = ThreadPoolUtils.runCallablesInParallel(executorService, fileExporters,
        // (int) TimeUnit.DAYS.toMinutes(1), 30);

        return null;
    }

    protected JobStatus waitForWorkflowStatus(String tenantId, String applicationId) {
        int retryOnException = 4;
        Job job;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId,
                        CustomerSpace.parse(tenantId).toString());
            } catch (Exception e) {
                log.error(String.format("Workflow job exception: %s", e.getMessage()), e);

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && job.getJobStatus().isTerminated()) {
                if (job.getJobStatus() == JobStatus.FAILED) {
                    log.error(applicationId + " Failed with ErrorCode " + job.getErrorCode() + ". \n"
                            + job.getErrorMsg());
                }
                if (job.getJobStatus() == JobStatus.CANCELLED) {
                    log.error(
                            applicationId + " Cancelled, Errorcode " + job.getErrorCode() + ". \n" + job.getErrorMsg());
                }
                if (job.getJobStatus() == JobStatus.SKIPPED) {
                    log.error(applicationId + " Skipped, Errorcode " + job.getErrorCode() + ". \n" + job.getErrorMsg());
                }
                if (job.getJobStatus() == JobStatus.COMPLETED) {
                    log.info(applicationId + " Completed!");
                }
                return job.getJobStatus();
            }
            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
