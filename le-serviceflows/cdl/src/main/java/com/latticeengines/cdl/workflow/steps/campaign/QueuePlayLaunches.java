package com.latticeengines.cdl.workflow.steps.campaign;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.QueuePlayLaunchesStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("queuePlayLaunches")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueuePlayLaunches extends BaseWorkflowStep<QueuePlayLaunchesStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(QueuePlayLaunches.class);

    @Inject
    private PlayProxy playProxy;

    @Override
    public void execute() {
        String customerSpace = configuration.getCustomerSpace().getTenantId();
        Play play = playProxy.getPlay(customerSpace, configuration.getPlayId(), false, false);
        PlayLaunchChannel channel = playProxy.getChannelById(customerSpace, configuration.getPlayId(), configuration.getChannelId());
        CDLExternalSystemName extSysName = channel.getLookupIdMap().getExternalSystemName();
        long accountsAdded = getCount(getLongValueFromContext(ACCOUNTS_ADDED));
        long accountsDeleted = getCount(getLongValueFromContext(ACCOUNTS_DELETED));
        long contactsAdded = getCount(getLongValueFromContext(CONTACTS_ADDED));
        long contactsDeleted = getCount(getLongValueFromContext(CONTACTS_DELETED));
        long fullContacts = getCount(getLongValueFromContext(FULL_CONTACTS));
        long accumulativeAccounts = getCount(getLongValueFromContext(ACCUMULATIVE_ACCOUNTS));
        long accumulativeContacts = getCount(getLongValueFromContext(ACCUMULATIVE_CONTACTS));
        log.info(String.format(
                "accountsAdded=%d, accountsDeleted=%d, accumulativeAccounts=%d, contactsAdded=%d, contactsDeleted=%d, fullContacts=%d, accumulativeContacts=%d",
                accountsAdded, accountsDeleted, accumulativeAccounts, contactsAdded, contactsDeleted, fullContacts,
                accumulativeContacts));
        // 4) Update current launch universe in the channel for the next delta
        // calculation
        PlayLaunch launch = null;
        if (StringUtils.isNotBlank(configuration.getLaunchId())) {
            launch = playProxy.getPlayLaunch(customerSpace, configuration.getPlayId(), configuration.getLaunchId());
            channel = playProxy.getPlayLaunchChannelFromPlayLaunch(customerSpace, configuration.getPlayId(),
                    configuration.getLaunchId());
        }
        channel.setCurrentLaunchedAccountUniverseTable(getObjectFromContext(FULL_ACCOUNTS_UNIVERSE, String.class));
        channel.setCurrentLaunchedContactUniverseTable(getObjectFromContext(FULL_CONTACTS_UNIVERSE, String.class));
        log.info(String.format(
                "Updating channel: %s with the FULL_ACCOUNTS_UNIVERSE table: %s and FULL_CONTACTS_UNIVERSE table: %s",
                configuration.getChannelId(), channel.getCurrentLaunchedAccountUniverseTable(),
                channel.getCurrentLaunchedContactUniverseTable()));
        playProxy.updatePlayLaunchChannel(customerSpace, configuration.getPlayId(), configuration.getChannelId(),
                channel, false);
        boolean deltaFound = wasDeltaDataFound(channel.getChannelConfig().getAudienceType(), extSysName);
        // For Eloqua, contactAdded should be fullContacts
        // Before migrating to Tray, Sureshot will do de-dup.
        if (CDLExternalSystemName.Eloqua.equals(extSysName)) {
            contactsAdded = fullContacts;
        }

        // When launchToDb, we don't create delete recommendations
        // set the deleted numbers to 0 for dashboard summaries
        boolean launchToDb = CDLExternalSystemName.LAUNCH_TO_DB.contains(extSysName);
        if (launchToDb) {
            log.info("launchToDb is true - setting deleted counts to 0");
            accountsDeleted = 0L;
            contactsDeleted = 0L;
        }

        if (deltaFound) {
            if (launch != null && launch.getLaunchState() == LaunchState.PreProcessing) {
                launch.setAddAccountsTable(getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class));
                launch.setCompleteContactsTable(getObjectFromContext(ADDED_ACCOUNTS_FULL_CONTACTS_TABLE, String.class));
                launch.setAddContactsTable(getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class));
                launch.setRemoveAccountsTable(getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class));
                launch.setRemoveContactsTable(getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class));
                long accountsSuppressed = launch.getAccountsSelected() - accumulativeAccounts;
                long contactsSuppressed = launch.getContactsSelected() - accumulativeContacts;
                log.info(String.format("For launch=%s, accountsSuppressed=%d, contactsSuppressed=%d",
                        launch.getLaunchId(), accountsSuppressed, contactsSuppressed));
                updatePlayLaunchStats(launch, accountsAdded, accountsDeleted, contactsAdded, contactsDeleted,
                        accumulativeAccounts, accumulativeContacts, accountsSuppressed, contactsSuppressed);
                log.info("PlayLaunch=" + JsonUtils.serialize(launch));
                playProxy.updatePlayLaunch(customerSpace, configuration.getPlayId(), configuration.getLaunchId(),
                        launch);
                log.info("Updated the scheduled Launch: " + configuration.getLaunchId() + " with delta tables ("
                        + getDeltaTables() + ")");
            } else if (launch != null && launch.getLaunchState() != LaunchState.PreProcessing) {
                log.warn("Launch found by LaunchId: " + configuration.getLaunchId() + " but in State: "
                        + launch.getLaunchState().name() + ". Hence queuing a new launch");
                launch = queueNewLaunch(play, accountsAdded, accountsDeleted, contactsAdded, contactsDeleted,
                        accumulativeAccounts, accumulativeContacts);
            } else {
                log.warn("No Launch found by LaunchId: " + configuration.getLaunchId() + ". Queuing a new launch");
                launch = queueNewLaunch(play, accountsAdded, accountsDeleted, contactsAdded, contactsDeleted,
                        accumulativeAccounts, accumulativeContacts);
            }
            Long launchWorkflowPid = playProxy.kickoffWorkflowForLaunch(configuration.getCustomerSpace().toString(),
                    configuration.getPlayId(), launch.getLaunchId());
            log.info("Kicked off delta launch workflow for the new Launch: " + launch.getId() + " with workflow PID ="
                    + launchWorkflowPid);
        } else {
            if (launch != null) {
                log.info(String.format(
                        "No Delta Found: Marking existing launch (%s) as skipped since no delta found for launch",
                        configuration.getLaunchId()));
                playProxy.updatePlayLaunch(customerSpace, configuration.getPlayId(), configuration.getLaunchId(),
                        LaunchState.Skipped);
            } else {
                log.info(String.format("No Delta Found: No launch queued for play %s, channel %s ",
                        configuration.getPlayId(), configuration.getChannelId()));
            }
        }
    }

    private PlayLaunch queueNewLaunch(Play play, long accountsAdded, long accountsDeleted, long contactsAdded,
            long contactsDeleted, long accumulativeAccounts, long accumulativeContacts) {
        PlayLaunch launch = new PlayLaunch();
        launch.setLaunchState(LaunchState.Queued);
        launch.setAddAccountsTable(getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class));
        launch.setCompleteContactsTable(getObjectFromContext(ADDED_ACCOUNTS_FULL_CONTACTS_TABLE, String.class));
        launch.setRemoveAccountsTable(getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class));
        launch.setAddContactsTable(getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class));
        launch.setRemoveContactsTable(getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class));
        long deltaPid = workflowJobEntityMgr.findByWorkflowId(jobId).getPid();
        launch.setParentDeltaWorkflowId(deltaPid);
        long accountsSuppressed = play.getTargetSegment().getAccounts() != null
                ? play.getTargetSegment().getAccounts() - accumulativeAccounts : 0L;
        long contactsSuppressed = play.getTargetSegment().getContacts() != null
                ? play.getTargetSegment().getContacts() - accumulativeContacts : 0L;
        log.info("totalAccounts=" + play.getTargetSegment().getAccounts());
        log.info("totalContacts=" + play.getTargetSegment().getContacts());
        log.info(String.format("accountsSuppressed=%d, contactsSuppressed=%d", accountsSuppressed, contactsSuppressed));
        updatePlayLaunchStats(launch, accountsAdded, accountsDeleted, contactsAdded, contactsDeleted,
                accumulativeAccounts, accumulativeContacts, accountsSuppressed, contactsSuppressed);
        log.info("Before create, PlayLaunch=" + JsonUtils.serialize(launch));
        launch = playProxy.createNewLaunchByPlayAndChannel(configuration.getCustomerSpace().toString(),
                configuration.getPlayId(), configuration.getChannelId(), true, launch);
        log.info("After create, PlayLaunch=" + JsonUtils.serialize(launch));
        log.info("Queued New Launch: " + launch.getId() + " with delta tables (" + getDeltaTables() + ")");

        return launch;
    }

    private void updatePlayLaunchStats(PlayLaunch launch, long accountsAdded, long accountsDeleted, long contactsAdded,
            long contactsDeleted, long accumulativeAccounts, long accumulativeContacts, long accountsSuppressed,
            long contactsSuppressed) {
        launch.setAccountsAdded(accountsAdded);
        launch.setAccountsDeleted(accountsDeleted);
        launch.setContactsAdded(contactsAdded);
        launch.setContactsDeleted(contactsDeleted);
        launch.setAccountsLaunched(accumulativeAccounts);
        launch.setContactsLaunched(accumulativeContacts);
        launch.setAccountsSuppressed(accountsSuppressed);
        launch.setContactsSuppressed(contactsSuppressed);
    }

    private long getCount(Long object) {
        return object != null ? object.longValue() : 0L;
    }

    private String getDeltaTables() {
        return (StringUtils.isNotBlank(getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class))
                ? (" AddedAccounts: " + getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class))
                : "") //
                + (StringUtils.isNotBlank(getObjectFromContext(ADDED_ACCOUNTS_FULL_CONTACTS_TABLE, String.class))
                        ? (" AddedCompleteContacts: "
                                + getObjectFromContext(ADDED_ACCOUNTS_FULL_CONTACTS_TABLE, String.class))
                        : "") //
                + (StringUtils.isNotBlank(getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class))
                        ? (" AddedContacts: " + getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class))
                        : "") //
                + (StringUtils.isNotBlank(getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class))
                        ? (" RemovedAccounts: " + getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class))
                        : "") //
                + (StringUtils.isNotBlank(getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class))
                        ? (" RemovedContacts: " + getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class))
                        : "");
    }

    private boolean wasDeltaDataFound(AudienceType audienceType, CDLExternalSystemName externalSystemName) {
        Map<String, Long> counts = getMapObjectFromContext(DELTA_TABLE_COUNTS, String.class, Long.class);
        switch (externalSystemName) {
        case Salesforce:
        case Eloqua:
            return MapUtils.isNotEmpty(counts) && //
                    (counts.getOrDefault(getAddDeltaTableContextKeyByAudienceType(audienceType), 0L) > 0L);
        case AWS_S3:
        case Marketo:
        case Facebook:
        case LinkedIn:
        case Outreach:
        case GoogleAds:
            return doesMapContainValues(counts, audienceType);
        case Adobe_Audience_Mgr:
            return doesMapContainValues(counts, AudienceType.CONTACTS);
        case MediaMath:
            return doesMapContainValues(counts, AudienceType.CONTACTS);
        case TradeDesk:
            return doesMapContainValues(counts, AudienceType.CONTACTS);
        case Verizon_Media:
            return doesMapContainValues(counts, AudienceType.CONTACTS);
        case Google_Display_N_Video_360:
            return doesMapContainValues(counts, AudienceType.CONTACTS);
        case AppNexus:
            return doesMapContainValues(counts, AudienceType.CONTACTS);
        default:
            return false;
        }
    }

    private boolean doesMapContainValues(Map<String, Long> counts, AudienceType audienceType) {
        return MapUtils.isNotEmpty(counts) && //
                (counts.getOrDefault(getAddDeltaTableContextKeyByAudienceType(audienceType), 0L) > 0L
                        || counts.getOrDefault(getRemoveDeltaTableContextKeyByAudienceType(audienceType), 0L) > 0L);
    }

    private String getAddDeltaTableContextKeyByAudienceType(AudienceType audienceType) {
        switch (audienceType) {
        case ACCOUNTS:
            return ADDED_ACCOUNTS_DELTA_TABLE;
        case CONTACTS:
            return ADDED_CONTACTS_DELTA_TABLE;
        default:
            return null;
        }
    }

    private String getRemoveDeltaTableContextKeyByAudienceType(AudienceType audienceType) {
        switch (audienceType) {
        case ACCOUNTS:
            return REMOVED_ACCOUNTS_DELTA_TABLE;
        case CONTACTS:
            return REMOVED_CONTACTS_DELTA_TABLE;
        default:
            return null;
        }
    }
}
