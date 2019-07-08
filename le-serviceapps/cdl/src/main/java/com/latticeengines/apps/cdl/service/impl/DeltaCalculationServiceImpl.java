package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.DeltaCalculationService;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("deltaCalculationService")
public class DeltaCalculationServiceImpl extends BaseRestApiProxy implements DeltaCalculationService {
    private static final Logger log = LoggerFactory.getLogger(DeltaCalculationServiceImpl.class);

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private WorkflowProxy workflowProxy;

    @Value("common.internal.app.url")
    private String internalAppUrl;

    private final String campaignDeltaCalculationUrlPrefix = "/customerspaces/{customerSpace}/plays/{playId}/channels/{channelId}/kickoff-delta-calculation";

    public DeltaCalculationServiceImpl() {
        super(PropertyUtils.getProperty("common.internal.app.url"), "cdl");
    }

    public Boolean triggerScheduledCampaigns() {
        if (StringUtils.isBlank(internalAppUrl)) {
            log.warn("Common internal app url not found, ignoring this job");
            return false;
        }

        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllScheduledChannels();

        log.info("Found " + channels.size() + " channels scheduled for launch");

        // ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("delta-calculation", 5);

        channels.forEach(c -> {
            try {
                String url = constructUrl(campaignDeltaCalculationUrlPrefix,
                        CustomerSpace.parse(c.getTenant().getId()).getTenantId(), c.getPlay().getName(), c.getId());
                String appId = post("Kicking off delta calculation", url, null, String.class);
                log.info("Queued a delta calculation for campaignId " + c.getId() + " : " + appId);
                waitForWorkflowStatus(c.getTenant().getId(), appId);
            } catch (Exception e) {
                log.error("Failed to Kick off delta calculation", e);
            }
        });
        return true;
    }

    private JobStatus waitForWorkflowStatus(String tenantId, String applicationId) {
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
