package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.CampaignLaunchSchedulingService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("campaignLaunchSchedulingService")
public class CampaignLaunchSchedulingServiceImpl extends BaseRestApiProxy implements CampaignLaunchSchedulingService {
    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchSchedulingServiceImpl.class);

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private BatonService batonService;

    @Autowired
    private AlertService alertService;

    @Value("common.internal.app.url")
    private String internalAppUrl;

    private final String campaignLaunchUrlPrefix = "/customerspaces/{customerSpace}/plays/{playId}/channels/{channelId}/kickoff-workflow?is-auto-launch=true";
    private final String setChannelScheduleUrlPrefix = "/customerspaces/{customerSpace}/plays//{playName}/channels/{channelId}/next-scheduled-date";

    private final String FAILURE_URL_KEY = "FAILURE_URL_KEY";
    private final String FAILURE_DESCRIPTION_KEY = "FAILURE_DESCRIPTION_KEY";
    private final String FAILURE_MESSAGE_KEY = "FAILURE_MESSAGE_KEY";
    private final String FAILURE_STACKTRACE_KEY = "FAILURE_STACKTRACE_KEY";

    public CampaignLaunchSchedulingServiceImpl() {
        super(PropertyUtils.getProperty("common.internal.app.url"), "cdl");
    }

    @Override
    public Boolean kickoffScheduledCampaigns() {
        if (StringUtils.isBlank(internalAppUrl)) {
            log.warn("Common internal app url not found, ignoring this job");
            return false;
        }

        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllValidScheduledChannels();

        List<PlayLaunchChannel> deltaEnabledTenantChannels = channels.stream().filter(c -> batonService
                .isEnabled(CustomerSpace.parse(c.getTenant().getId()), LatticeFeatureFlag.ENABLE_DELTA_CALCULATION))
                .collect(Collectors.toList());

        long inValidDeltaLaunchChannels = channels.stream().filter(c -> c.getLaunchType() == LaunchType.DELTA)
                .filter(c -> !batonService.isEnabled(CustomerSpace.parse(c.getTenant().getId()),
                        LatticeFeatureFlag.ENABLE_DELTA_CALCULATION))
                .count();
        if (inValidDeltaLaunchChannels > 0) {
            log.warn(inValidDeltaLaunchChannels
                    + " channels found scheduled for delta launches, in tenants inactive for delta calculation,"
                    + " these channels will be skipped");
        }

        List<PlayLaunchChannel> deltaDisabledTenantChannels = channels.stream()
                .filter(c -> c.getLaunchType() == LaunchType.FULL)
                .filter(c -> !batonService.isEnabled(CustomerSpace.parse(c.getTenant().getId()),
                        LatticeFeatureFlag.ENABLE_DELTA_CALCULATION))
                .collect(Collectors.toList());

        log.info(String.format(
                "Found %d channels scheduled for launch, Full Launches in Delta Disabled Tenants: %d, Delta Launches in Delta Disabled Tenants: %d,Launches is Delta Enabled tenants: %d",
                channels.size(), deltaDisabledTenantChannels.size(), inValidDeltaLaunchChannels,
                deltaEnabledTenantChannels.size()));

        long successfullyQueuedForDelta = deltaEnabledTenantChannels.stream().map(this::queueNewDeltaCalculationJob)
                .filter(x -> x).count();

        long successfullyQueuedForFull = deltaDisabledTenantChannels.stream().map(this::queueNewFullCampaignLaunch)
                .filter(x -> x).count();

        log.info(String.format(
                "Total Delta Calculation Jobs to be Scheduled: %d, Queued Delta: %d, Queued Full Launches: %d Job Submissions failed: %d",
                channels.size(), //
                successfullyQueuedForDelta, //
                successfullyQueuedForFull, channels.size() - successfullyQueuedForDelta - successfullyQueuedForFull));
        return true;
    }

    private boolean queueNewDeltaCalculationJob(PlayLaunchChannel channel) {
        return getRetryTemplate().execute(context -> {
            String url = constructUrl(campaignLaunchUrlPrefix,
                    CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), channel.getPlay().getName(),
                    channel.getId());
            try {
                if (context.getRetryCount() > 0) {
                    log.info(String.format("(attempt=%d) to kick off delta calculation", context.getRetryCount() + 1));
                    log.warn("Previous failure:", context.getLastThrowable());
                }
                Long workflowPid = post("Kicking off delta calculation", url, null, Long.class);
                log.info("Kicked off  a delta calculation job for campaignId " + channel.getPlay().getName()
                        + ", Channel ID: " + channel.getId() + " WorkflowPid: " + workflowPid);
                return true;
            } catch (Exception e) {
                log.error("Failed to Kick off delta calculation for channel: " + channel.getId() + " \n", e);
                context.setAttribute(FAILURE_URL_KEY, url);
                String failureDescription = String.format(
                        "Failed to kick off scheduled campaign launch for Campaign: %s, Channel: %s (ID: %s) of tenant %s",
                        channel.getPlay().getDisplayName(), channel.getLookupIdMap().getExternalSystemName(),
                        channel.getId(), channel.getTenant().getName());
                context.setAttribute(FAILURE_DESCRIPTION_KEY, failureDescription);
                context.setAttribute(FAILURE_MESSAGE_KEY, e.getMessage());
                context.setAttribute(FAILURE_STACKTRACE_KEY, e.getStackTrace());
                return false;
            } finally {
                String channelScheduleUrl = constructUrl(setChannelScheduleUrlPrefix,
                        CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), channel.getPlay().getName(),
                        channel.getId());
                log.info("Setting Next Scheduled Date for campaignId " + channel.getPlay().getName() + ", Channel ID: "
                        + channel.getId());
                patch("Setting Next Scheduled Date", channelScheduleUrl, null, PlayLaunchChannel.class);
            }
        });
    }

    private boolean queueNewFullCampaignLaunch(PlayLaunchChannel channel) {
        return getRetryTemplate().execute(context -> {
            String url = constructUrl(campaignLaunchUrlPrefix,
                    CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), channel.getPlay().getName(),
                    channel.getId());
            context.setAttribute("url", url);
            try {
                if (context.getRetryCount() > 0) {
                    log.info(String.format("(attempt=%d) to kick off campaign launch workflow",
                            context.getRetryCount() + 1));
                    log.warn("Previous failure:", context.getLastThrowable());
                }
                PlayLaunch launch = post("Kicking off Campaign Launch", url, null, PlayLaunch.class);
                log.info("Kicked off a Campaign Launch for campaignId " + channel.getPlay().getName() + ", Channel ID: "
                        + channel.getId() + " Launch Id: " + launch.getLaunchId() + "  WorflowPid: "
                        + launch.getLaunchWorkflowId());
                return true;
            } catch (Exception e) {
                log.error("Failed to Kick off Campaign Launch for channel: " + channel.getId() + " \n", e);
                context.setAttribute(FAILURE_URL_KEY, url);
                String failureDescription = String.format(
                        "Failed to kick off scheduled Campaign Launch for Campaign: %s, Channel: %s (ID: %s) of tenant %s due to \n%s",
                        channel.getPlay().getDisplayName(), channel.getLookupIdMap().getExternalSystemName(),
                        channel.getId(), channel.getTenant().getName(), e.getMessage());
                context.setAttribute(FAILURE_DESCRIPTION_KEY, failureDescription);
                context.setAttribute(FAILURE_MESSAGE_KEY, e.getMessage());
                context.setAttribute(FAILURE_STACKTRACE_KEY, e.getStackTrace());
                return false;
            } finally {
                String channelScheduleUrl = constructUrl(setChannelScheduleUrlPrefix,
                        CustomerSpace.parse(channel.getTenant().getId()).getTenantId(), channel.getPlay().getName(),
                        channel.getId());
                log.info("Setting Next Scheduled Date for campaignId " + channel.getPlay().getName() + ", Channel ID: "
                        + channel.getId());
                patch("Setting Next Scheduled Date", channelScheduleUrl, null, PlayLaunchChannel.class);
            }
        });
    }

    private RetryTemplate getRetryTemplate() {
        RetryTemplate template = RetryUtils.getExponentialBackoffRetryTemplate(3, 2000L, 2.0D, null);
        template.registerListener(getLaunchSchedulingFailureListener());
        return template;
    }

    private RetryListener getLaunchSchedulingFailureListener() {
        return new RetryListenerSupport() {
            @Override
            public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
                    Throwable throwable) {
                // Send a PagerDuty
                List<BasicNameValuePair> details = new ArrayList<>();
                details.add(new BasicNameValuePair(FAILURE_URL_KEY, (String) context.getAttribute(FAILURE_URL_KEY)));
                details.add(new BasicNameValuePair(FAILURE_DESCRIPTION_KEY,
                        (String) context.getAttribute(FAILURE_DESCRIPTION_KEY)));
                details.add(new BasicNameValuePair(FAILURE_MESSAGE_KEY,
                        (String) context.getAttribute(FAILURE_MESSAGE_KEY)));
                details.add(new BasicNameValuePair(FAILURE_STACKTRACE_KEY,
                        (String) context.getAttribute(FAILURE_STACKTRACE_KEY)));
                alertService.triggerCriticalEvent((String) context.getAttribute(FAILURE_STACKTRACE_KEY),
                        (String) context.getAttribute(FAILURE_URL_KEY), (String) context.getAttribute(FAILURE_URL_KEY),
                        details);
                super.close(context, callback, throwable);
            }
        };
    }

}
