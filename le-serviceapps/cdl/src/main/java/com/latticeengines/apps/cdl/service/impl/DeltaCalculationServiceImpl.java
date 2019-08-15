package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.DeltaCalculationService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
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
    private BatonService batonService;

    @Inject
    private WorkflowProxy workflowProxy;

    @Value("common.internal.app.url")
    private String internalAppUrl;

    @Value("${cdl.delta.calculation.maximum.job.count}")
    private int maxJobs;

    private final String campaignDeltaCalculationUrlPrefix = "/customerspaces/{customerSpace}/plays/{playId}/channels/{channelId}/kickoff-delta-calculation";

    public DeltaCalculationServiceImpl() {
        super(PropertyUtils.getProperty("common.internal.app.url"), "cdl");
    }

    @Override
    public Boolean triggerScheduledCampaigns() {
        if (StringUtils.isBlank(internalAppUrl)) {
            log.warn("Common internal app url not found, ignoring this job");
            return false;
        }

        if (!batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS)) {
            log.info("Cannot auto queue PlayLaunch if always on feature flag is disabled");
            return false;
        }

        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllValidScheduledChannels();

        log.info("Found " + channels.size() + " channels scheduled for launch");

        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("delta-calculation", maxJobs);

        List<Callable<JobStatus>> deltaCalculationJobs = channels.stream().map(DeltaCalculationJobWrapper::new)
                .collect(Collectors.toList());

        List<JobStatus> statuses = ThreadPoolUtils.runCallablesInParallel(executorService, deltaCalculationJobs,
                (int) TimeUnit.DAYS.toMinutes(1), (int) TimeUnit.HOURS.toSeconds(2));

        log.info(String.format(
                "Total Delta Calculation Jobs queued: %s, Completed: %s, Unsuccessful: %s, Job Submission failed: %s",
                statuses.size(), //
                statuses.stream().filter(j -> j == JobStatus.COMPLETED).count(), //
                statuses.stream().filter(Objects::nonNull).filter(JobStatus::isUnsuccessful).count(), //
                statuses.stream().filter(Objects::isNull).count()));
        return true;
    }

    private class DeltaCalculationJobWrapper implements Callable<JobStatus> {

        private PlayLaunchChannel channel;

        DeltaCalculationJobWrapper(PlayLaunchChannel channel) {
            this.channel = channel;
        }

        @Override
        public JobStatus call() {
            try {

                String url = constructUrl(campaignDeltaCalculationUrlPrefix,
                        CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), channel.getPlay().getName(),
                        channel.getId());
                String appId = post("Kicking off delta calculation", url, null, String.class);
                log.info("Queued a delta calculation job for campaignId " + channel.getPlay().getName()
                        + ", Channel ID: " + channel.getId() + " : " + appId);
                return waitForWorkflowStatus(channel.getTenant().getId(), appId);
            } catch (Exception e) {
                log.error("Failed to Kick off delta calculation", e);
                return null;
            }
        }
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
