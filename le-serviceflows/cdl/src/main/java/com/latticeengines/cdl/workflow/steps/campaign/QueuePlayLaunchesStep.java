package com.latticeengines.cdl.workflow.steps.campaign;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.QueuePlayLaunchesStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("queuePlayLaunchesStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueuePlayLaunchesStep extends BaseWorkflowStep<QueuePlayLaunchesStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(QueuePlayLaunchesStep.class);

    @Inject
    private PlayProxy playProxy;

    @Override
    public void execute() {
        // 4) Update current launch universe in the channel for the next delta
        // calculation
        PlayLaunchChannel channel = playProxy.getChannelById(configuration.getCustomerSpace().getTenantId(),
                configuration.getPlayId(), configuration.getChannelId());
        channel.setCurrentLaunchedAccountUniverseTable(getObjectFromContext(FULL_ACCOUNTS_UNIVERSE, String.class));
        channel.setCurrentLaunchedContactUniverseTable(getObjectFromContext(FULL_CONTACTS_UNIVERSE, String.class));
        log.info(String.format(
                "Updating channel: %s with the FULL_ACCOUNTS_UNIVERSE table: %s and FULL_CONTACTS_UNIVERSE table: %s",
                configuration.getChannelId(), channel.getCurrentLaunchedAccountUniverseTable(),
                channel.getCurrentLaunchedContactUniverseTable()));
        playProxy.updatePlayLaunchChannel(configuration.getCustomerSpace().getTenantId(), configuration.getPlayId(),
                configuration.getChannelId(), channel, false);

        log.info("Queueing the scheduled PlayLaunch");
        if (StringUtils.isNotBlank(configuration.getLaunchId())) {
            PlayLaunch launch = playProxy.getPlayLaunch(configuration.getCustomerSpace().getTenantId(),
                    configuration.getPlayId(), configuration.getLaunchId());
            if (launch != null && launch.getLaunchState() == LaunchState.UnLaunched) {
                launch.setAddAccountsTable(getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class));
                launch.setAddContactsTable(getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class));
                launch.setRemoveAccountsTable(getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class));
                launch.setRemoveContactsTable(getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class));
                playProxy.updatePlayLaunch(configuration.getCustomerSpace().getTenantId(), configuration.getPlayId(),
                        configuration.getLaunchId(), launch);
                log.info("Updated the scheduled Launch: " + configuration.getLaunchId() + " with delta tables ("
                        + getDeltaTables() + ")");
            } else if (launch != null && launch.getLaunchState() != LaunchState.UnLaunched) {
                log.warn("Launch found by LaunchId: " + configuration.getLaunchId() + " but in State: "
                        + launch.getLaunchState().name() + ". Hence queuing a new launch");
                queueNewLaunch();
            } else {
                log.warn("No Launch found by LaunchId: " + configuration.getLaunchId() + ". Queuing a new launch");
                queueNewLaunch();
            }
        } else {
            queueNewLaunch();
        }

        playProxy.setNextScheduledTimeForChannel(configuration.getCustomerSpace().toString(), configuration.getPlayId(),
                configuration.getChannelId());
    }

    private void queueNewLaunch() {
        PlayLaunch launch = playProxy.queueNewLaunchByPlayAndChannel(configuration.getCustomerSpace().toString(),
                configuration.getPlayId(), configuration.getChannelId(),
                getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class),
                getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class),
                getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class),
                getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class), true);
        log.info("Queued New Launch: " + launch.getId() + " with delta tables (" + getDeltaTables() + ")");
    }

    private String getDeltaTables() {
        return (StringUtils.isNotBlank(getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class))
                ? ("AddedAccounts: " + getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class))
                : "") //
                + (StringUtils.isNotBlank(getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class))
                        ? ("AddedAccounts: " + getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class))
                        : "") //
                + (StringUtils.isNotBlank(getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class))
                        ? ("AddedAccounts: " + getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class))
                        : "") //
                + (StringUtils.isNotBlank(getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class))
                        ? ("AddedAccounts: " + getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class))
                        : "");

    }
}
