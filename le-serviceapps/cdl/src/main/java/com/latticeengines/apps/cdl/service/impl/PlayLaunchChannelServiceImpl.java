package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.EventDetail;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.ratings.coverage.RatingBucketCoverage;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.PlayUtils;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.EmailProxy;

@Component("playLaunchChannelService")
public class PlayLaunchChannelServiceImpl implements PlayLaunchChannelService {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchChannelServiceImpl.class);

    @Inject
    private PlayService playService;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private RatingCoverageService ratingCoverageService;

    @Inject
    private BatonService batonService;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private EmailProxy emailProxy;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Value("${cdl.play.service.default.types.user}")
    private String serviceUser;

    @Override
    public PlayLaunchChannel create(String playName, PlayLaunchChannel playLaunchChannel) {
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playName });
        }
        playLaunchChannel.setPlay(play);
        playLaunchChannel.setTenant(MultiTenantContext.getTenant());
        playLaunchChannel.setTenantId(MultiTenantContext.getTenant().getPid());
        playLaunchChannel = playLaunchChannelEntityMgr.createPlayLaunchChannel(playLaunchChannel);
        playLaunchChannel.setPlay(play); // ensure play exists if used in
                                         // resource
        return playLaunchChannel;
    }

    @Override
    public PlayLaunchChannel update(String playName, PlayLaunchChannel playLaunchChannel) {
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playName });
        }
        playLaunchChannel.setPlay(play);
        PlayLaunchChannel retrievedPlayLaunchChannel = findById(playLaunchChannel.getId());
        if (retrievedPlayLaunchChannel == null) {
            throw new NullPointerException("Cannot find Play Launch Channel for given play channel id");
        }
        if (!retrievedPlayLaunchChannel.getPlay().getName().equals(playLaunchChannel.getPlay().getName())) {
            throw new LedpException(LedpCode.LEDP_18225, new String[] { retrievedPlayLaunchChannel.getPlay().getName(),
                    playLaunchChannel.getPlay().getName() });
        }
        playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrievedPlayLaunchChannel, playLaunchChannel);
        retrievedPlayLaunchChannel.setPlay(play); // ensure play exists if used
                                                  // in resource
        retrievedPlayLaunchChannel
                .setLastLaunch(playLaunchService.findLatestByChannel(retrievedPlayLaunchChannel.getPid()));
        return retrievedPlayLaunchChannel;
    }

    @Override
    public PlayLaunchChannel updateNextScheduledDate(String playId, String channelId) {
        Play play = playService.getPlayByName(playId, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playId });
        }
        PlayLaunchChannel channel = findById(channelId, true);
        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Channel found with id: " + channelId });
        }
        channel.setPlay(play);

        if (channel.getIsAlwaysOn() && StringUtils.isBlank(channel.getCronScheduleExpression())) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Cannot schedule next launch for channel : " + channelId + " since always on is turned off" });
        }

        Date nextLaunchDate = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        if (channel.getExpirationDate() == null)
            log.warn(String.format("Expiration Date Null for Channel: %s", channel.getId()));
        if (channel.getExpirationDate() != null && nextLaunchDate.after(channel.getExpirationDate())) {
            log.info(String.format(
                    "Channel: %s has expired turning auto launches off (Expiration Date: %s, Next Scheduled Launch Date: %s)",
                    channel.getId(), channel.getExpirationDate().toString(), nextLaunchDate.toString()));
            channel.setIsAlwaysOn(false);
        } else {
            channel.setNextScheduledLaunch(PlayLaunchChannel.getNextDateFromCronExpression(channel));
        }
        playLaunchChannelEntityMgr.update(channel);
        sendEmailIfSecondToLastLaunch(channel);

        return channel;
    }

    @Override
    public PlayLaunchChannel updateLastDeltaWorkflowId(String playId, String channelId, Long workflowPid) {
        if (workflowPid == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Unable to update null workflowPid" });
        }

        Play play = playService.getPlayByName(playId, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playId });
        }

        PlayLaunchChannel channel = findById(channelId, true);
        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Channel found with id: " + channelId });
        }

        channel.setPlay(play);
        channel.setLastDeltaWorkflowId(workflowPid);
        playLaunchChannelEntityMgr.update(channel);
        return channel;
    }

    @Override
    public List<PlayLaunchChannel> findByIsAlwaysOnTrue() {
        return playLaunchChannelEntityMgr.findByIsAlwaysOnTrue();
    }

    @Override
    public PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playName, String lookupId) {
        return playLaunchChannelEntityMgr.findByPlayNameAndLookupIdMapId(playName, lookupId);
    }

    @Override
    public PlayLaunchChannel findById(String channelId) {
        return findById(channelId, false);
    }

    @Override
    public PlayLaunchChannel findChannelAndPlayById(String channelId) {
        return playLaunchChannelEntityMgr.findChannelAndPlayById(channelId);
    }

    @Override
    public PlayLaunchChannel findById(String channelId, boolean useWriterRepo) {
        PlayLaunchChannel channel = playLaunchChannelEntityMgr.findById(channelId, useWriterRepo);
        if (channel != null)
            channel.setLastLaunch(playLaunchService.findLatestByChannel(channel.getPid()));
        return channel;
    }

    @Override
    public List<PlayLaunchChannel> getPlayLaunchChannels(String playName, Boolean includeUnlaunchedChannels) {
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.findByPlayName(playName);
        for (PlayLaunchChannel playLaunchChannel : channels) {
            PlayLaunch playLaunch = playLaunchService.findLatestByChannel(playLaunchChannel.getPid());
            playLaunchChannel.setLastLaunch(playLaunch);
            if (playLaunch != null && playLaunch.getLaunchId() != null) {
                DataIntegrationStatusMessage audienceState = dataIntegrationStatusMonitoringEntityMgr
                        .getLatestMessageByLaunchId(playLaunch.getLaunchId());
                if (audienceState != null && audienceState.getEventDetail() != null) {
                    EventDetail details = audienceState.getEventDetail();
                    if (details instanceof ProgressEventDetail) {
                        playLaunch.setAudienceState(((ProgressEventDetail) details).getStatus());
                    }
                }
            }
        }
        if (includeUnlaunchedChannels) {
            addUnlaunchedChannels(channels);
        }
        channels = removeDisconnectedChannels(channels);
        return channels;
    }

    @Override
    public void deleteByChannelId(String channelId, boolean hardDelete) {
        if (StringUtils.isBlank(channelId)) {
            throw new LedpException(LedpCode.LEDP_18228);
        }
        playLaunchChannelEntityMgr.deleteByChannelId(channelId, hardDelete);
    }

    @Override
    public PlayLaunch createNewLaunchByPlayAndChannel(Play play, PlayLaunchChannel playLaunchChannel, PlayLaunch launch,
            boolean autoLaunch) {
        if (launch != null && !launch.getLaunchState().isInitial()) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Cannot create a new launch with state: " + launch.getLaunchState().name() });
        }
        runValidations(MultiTenantContext.getTenant().getId(), play, playLaunchChannel);
        PlayLaunch newLaunch = createDefaultLaunchFromPlayAndChannel(play, playLaunchChannel, LaunchState.Queued,
                autoLaunch);

        if (launch != null) {
            newLaunch.merge(launch);
        }

        playLaunchService.create(newLaunch);
        return newLaunch;
    }

    private List<PlayLaunchChannel> removeDisconnectedChannels(List<PlayLaunchChannel> channelList) {
        channelList.stream().filter(channel -> channel.getLookupIdMap().getIsRegistered()).collect(Collectors.toList());
        return channelList;
    }

    private PlayLaunch createDefaultLaunchFromPlayAndChannel(Play play, PlayLaunchChannel playLaunchChannel,
            LaunchState state, boolean isAutoLaunch) {
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setTenant(MultiTenantContext.getTenant());
        playLaunch.setLaunchId(PlayLaunch.generateLaunchId());
        playLaunch.setUpdatedBy(isAutoLaunch ? serviceUser : playLaunchChannel.getUpdatedBy());
        playLaunch.setCreatedBy(isAutoLaunch ? serviceUser : playLaunchChannel.getUpdatedBy());
        playLaunch.setPlay(play);
        playLaunch.setPlayLaunchChannel(playLaunchChannel);
        playLaunch.setLaunchState(state != null ? state : LaunchState.Canceled);
        playLaunch.setTopNCount(playLaunchChannel.getMaxEntitiesToLaunch());
        playLaunch.setBucketsToLaunch(playLaunchChannel.getBucketsToLaunch());
        playLaunch.setLaunchUnscored(playLaunchChannel.getLaunchUnscored());
        playLaunch.setDestinationOrgId(playLaunchChannel.getLookupIdMap().getOrgId());
        playLaunch.setDestinationSysType(playLaunchChannel.getLookupIdMap().getExternalSystemType());
        playLaunch.setDestinationSysName(playLaunchChannel.getLookupIdMap().getExternalSystemName());
        playLaunch.setDestinationAccountId(playLaunchChannel.getLookupIdMap().getAccountId());
        playLaunch.setDestinationContactId(playLaunchChannel.getLookupIdMap().getContactId());
        playLaunch.setTableName(createTable());
        playLaunch.setLaunchType(playLaunchChannel.getLaunchType());
        playLaunchChannel.getChannelConfig().populateLaunchFromChannelConfig(playLaunch);
        playLaunch.setChannelConfig(playLaunchChannel.getChannelConfig());
        playLaunch.setScheduledLaunch(isAutoLaunch);
        playLaunch.setTapType(play.getTapType());

        Long totalAvailableRatedAccounts = play.getTargetSegment().getAccounts();
        Long totalAvailableContacts = play.getTargetSegment().getContacts();
        playLaunch.setAccountsSelected(totalAvailableRatedAccounts != null ? totalAvailableRatedAccounts : 0L);
        playLaunch.setAccountsSuppressed(0L);
        playLaunch.setAccountsErrored(0L);
        playLaunch.setAccountsLaunched(0L);
        playLaunch.setAccountsAdded(0L);
        playLaunch.setAccountsDeleted(0L);
        playLaunch.setContactsSelected(totalAvailableContacts != null ? totalAvailableContacts : 0L);
        playLaunch.setContactsLaunched(0L);
        playLaunch.setContactsAdded(0L);
        playLaunch.setContactsDeleted(0L);
        playLaunch.setContactsSuppressed(0L);
        playLaunch.setContactsErrored(0L);
        return playLaunch;
    }

    @Override
    public PlayLaunchChannel updateAudience(String audienceId, String audienceName, String playLaunchId) {
        PlayLaunch playLaunchRetrieved = playLaunchService.findByLaunchId(playLaunchId, true);
        if (playLaunchRetrieved != null) {
            PlayLaunchChannel playLaunchChannel = playLaunchRetrieved.getPlayLaunchChannel();
            Play play = playLaunchRetrieved.getPlay();
            if (playLaunchChannel != null && play != null) {
                ChannelConfig channlConfigUpdated = playLaunchChannel.getChannelConfig();
                channlConfigUpdated.setAudienceName(audienceName);
                channlConfigUpdated.setAudienceId(audienceId);
                playLaunchChannel.setChannelConfig(channlConfigUpdated);
                playLaunchChannel = update(play.getName(), playLaunchChannel);
                return playLaunchChannel;
            }
            throw new LedpException(LedpCode.LEDP_18237, new String[] { playLaunchId, (play == null ? "true" : "false"),
                    (playLaunchChannel == null ? "true" : "false") });
        }
        throw new LedpException(LedpCode.LEDP_18238, new String[] { audienceName, audienceId });
    }

    private void runValidations(String customerSpace, Play play, PlayLaunchChannel playLaunchChannel) {
        PlayUtils.validatePlay(play);
        validatePlayAndChannelBeforeLaunch(customerSpace, play, playLaunchChannel);
    }

    private List<PlayLaunchChannel> addUnlaunchedChannels(List<PlayLaunchChannel> channels) {
        List<LookupIdMap> allConnections = lookupIdMappingEntityMgr.getLookupIdMappings(null, null, true);
        if (CollectionUtils.isNotEmpty(allConnections)) {
            allConnections.forEach(mapping -> addToListIfDoesntExist(mapping, channels));
        }
        return channels;
    }

    private void addToListIfDoesntExist(LookupIdMap mapping, List<PlayLaunchChannel> channels) {
        String configId = mapping.getId();
        if (!mapping.getIsRegistered()) {
            return;
        }
        for (PlayLaunchChannel channel : channels) {
            if (channel.getLookupIdMap().getId().equals(configId)) {
                return;
            }
        }
        PlayLaunchChannel newChannel = new PlayLaunchChannel();
        newChannel.setLookupIdMap(mapping);
        channels.add(newChannel);
    }

    private String createTable() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        Tenant tenant = MultiTenantContext.getTenant();
        com.latticeengines.domain.exposed.metadata.Table generatedRecommendationTable = new com.latticeengines.domain.exposed.metadata.Table();
        generatedRecommendationTable.addAttributes(Recommendation.getSchemaAttributes());

        String tableName = "play_launch_" + UUID.randomUUID().toString().replaceAll("-", "_");
        generatedRecommendationTable.setName(tableName);
        generatedRecommendationTable.setTableType(TableType.DATATABLE);

        generatedRecommendationTable.setDisplayName("Play Launch recommendation");
        generatedRecommendationTable.setTenant(tenant);
        generatedRecommendationTable.setTenantId(tenant.getPid());
        generatedRecommendationTable.setMarkedForPurge(false);
        metadataProxy.createTable(customerSpace.toString(), tableName, generatedRecommendationTable);

        generatedRecommendationTable = metadataProxy.getTable(customerSpace.toString(), tableName);

        return generatedRecommendationTable.getName();
    }

    private void validatePlayAndChannelBeforeLaunch(String tenantId, Play play, PlayLaunchChannel channel) {
        if (play.getRatingEngine() == null) {
            return;
        }

        RatingEngine ratingEngine = play.getRatingEngine();
        ratingEngine = ratingEngineService.getRatingEngineById(ratingEngine.getId(), false);
        play.setRatingEngine(ratingEngine);

        if (channel.getLookupIdMap() == null || channel.getLookupIdMap().getExternalSystemType() == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "No destination system selected for the channel for play: " + play.getName() });
        }

        if (channel.getChannelConfig().isSuppressAccountsWithoutLookupId()
                && StringUtils.isBlank(channel.getLookupIdMap().getAccountId())) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Cannot restrict accounts with null Ids if account id has not been set up for selected Connection" });
        }

        RatingEnginesCoverageRequest coverageRequest = new RatingEnginesCoverageRequest();
        coverageRequest.setRatingEngineIds(Collections.singletonList(play.getRatingEngine().getId()));
        coverageRequest.setRestrictNullLookupId(channel.getChannelConfig().isSuppressAccountsWithoutLookupId());
        coverageRequest.setLookupId(channel.getLookupIdMap().getAccountId());
        RatingEnginesCoverageResponse coverageResponse = ratingCoverageService.getRatingCoveragesForSegment(
                CustomerSpace.parse(tenantId).toString(), play.getTargetSegment().getName(), coverageRequest);

        if (coverageResponse == null || MapUtils.isNotEmpty(coverageResponse.getErrorMap())) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Unable to validate validity of launch targets due to internal Error, please retry later" });
        }

        Long accountsToLaunch = coverageResponse.getRatingModelsCoverageMap().get(play.getRatingEngine().getId())
                .getBucketCoverageCounts().stream()
                .filter(ratingBucket -> channel.getBucketsToLaunch()
                        .contains(RatingBucketName.valueOf(ratingBucket.getBucket())))
                .map(RatingBucketCoverage::getCount).reduce(0L, (a, b) -> a + b);

        accountsToLaunch = accountsToLaunch
                + (channel.getLaunchUnscored()
                        ? coverageResponse.getRatingModelsCoverageMap().get(play.getRatingEngine().getId())
                                .getUnscoredAccountCount()
                        : 0L);

        if (accountsToLaunch <= 0L) {
            throw new LedpException(LedpCode.LEDP_18176, new String[] { play.getName() });
        }
    }

    private void sendEmailIfSecondToLastLaunch(PlayLaunchChannel channel) {
        Date nextNextLaunchDate = PlayLaunchChannel.getNextDateFromCronExpression(channel,
                channel.getNextScheduledLaunch());
        if (channel.getExpirationDate() != null && nextNextLaunchDate.after(channel.getExpirationDate())) {
            try {
                emailProxy.sendPlayLaunchChannelExpiringEmail(MultiTenantContext.getTenant().getId(), channel);
            } catch (Exception e) {
                log.error("Can not send always on campaign is expiring email: " + e.getMessage());
            }
        }
    }

    @Override
    public void updateAttributeSetNameToDefault(String attributeSetName) {
        playLaunchChannelEntityMgr.updateAttributeSetNameToDefault(attributeSetName);
    }

    @Override
    public void updatePreviousLaunchedAccountUniverseWithCurrent(PlayLaunchChannel channel) {
        channel.setPreviousLaunchedAccountUniverseTable(channel.getCurrentLaunchedAccountUniverseTable());
        playLaunchChannelEntityMgr.update(channel);
    }

    @Override
    public void updatePreviousLaunchedContactUniverseWithCurrent(PlayLaunchChannel channel) {
        channel.setPreviousLaunchedContactUniverseTable(channel.getCurrentLaunchedContactUniverseTable());
        playLaunchChannelEntityMgr.update(channel);
    }

    @Override
    public PlayLaunchChannel recoverLaunchUniverse(PlayLaunchChannel channel) {
        PlayLaunchChannel retrievedChannel = findById(channel.getId());

        return playLaunchChannelEntityMgr.recoverLaunchUniverse(retrievedChannel, channel);
    }

}
