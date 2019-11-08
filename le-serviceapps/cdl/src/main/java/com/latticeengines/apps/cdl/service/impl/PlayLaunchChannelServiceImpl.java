package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
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
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.ratings.coverage.RatingBucketCoverage;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.PlayUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("playLaunchChannelService")
public class PlayLaunchChannelServiceImpl implements PlayLaunchChannelService {

    private static Logger log = LoggerFactory.getLogger(PlayLaunchChannelServiceImpl.class);

    @Inject
    private PlayService playService;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private PlayLaunchEntityMgr playLaunchEntityMgr;

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
        playLaunchChannel.setPlay(play); // ensure play exists if used in resource
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
        retrievedPlayLaunchChannel.setPlay(play); // ensure play exists if used in resource
        retrievedPlayLaunchChannel
                .setLastLaunch(playLaunchEntityMgr.findLatestByChannel(retrievedPlayLaunchChannel.getPid()));
        return retrievedPlayLaunchChannel;
    }

    @Override
    public PlayLaunchChannel updateNextScheduledDate(String playId, String channelId) {
        Play play = playService.getPlayByName(playId, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playId });
        }
        PlayLaunchChannel channel = findById(channelId);
        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Channel found with id: " + channelId });
        }
        channel.setPlay(play);
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

        PlayLaunchChannel channel = findById(channelId);
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
        return playLaunchChannelEntityMgr.findById(channelId);
    }

    @Override
    public List<PlayLaunchChannel> getPlayLaunchChannels(String playName, Boolean includeUnlaunchedChannels) {
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.findByPlayName(playName);
        for (PlayLaunchChannel playLaunchChannel : channels) {
            playLaunchChannel.setLastLaunch(playLaunchService.findLatestByChannel(playLaunchChannel.getPid()));
        }
        if (includeUnlaunchedChannels) {
            addUnlaunchedChannels(channels);
        }
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
    public PlayLaunch queueNewLaunchForChannel(Play play, PlayLaunchChannel playLaunchChannel, String addAccountTable,
            String completeContactsTable, String removeAccountsTable, String addContactsTable,
            String removeContactsTable, boolean autoLaunch) {
        runValidations(MultiTenantContext.getTenant().getId(), play, playLaunchChannel);
        PlayLaunch playLaunch = createLaunchFromPlayAndChannel(play, playLaunchChannel, LaunchState.Queued, autoLaunch);

        playLaunch.setLaunchState(LaunchState.Queued);
        playLaunch.setAddAccountsTable(addAccountTable);
        playLaunch.setCompleteContactsTable(completeContactsTable);
        playLaunch.setRemoveAccountsTable(removeAccountsTable);
        playLaunch.setAddContactsTable(addContactsTable);
        playLaunch.setRemoveContactsTable(removeContactsTable);

        playLaunchEntityMgr.create(playLaunch);
        playLaunchChannel.setLastLaunch(playLaunch);
        return playLaunch;
    }

    @Override
    public PlayLaunch createNewLaunchForChannelByState(Play play, PlayLaunchChannel playLaunchChannel,
            LaunchState state, boolean isAutoLaunch) {
        runValidations(MultiTenantContext.getTenant().getId(), play, playLaunchChannel);
        PlayLaunch playLaunch = createLaunchFromPlayAndChannel(play, playLaunchChannel, state, isAutoLaunch);
        playLaunchEntityMgr.create(playLaunch);
        playLaunchChannel.setLastLaunch(playLaunch);
        return playLaunch;
    }

    private PlayLaunch createLaunchFromPlayAndChannel(Play play, PlayLaunchChannel playLaunchChannel, LaunchState state,
            boolean autoLaunch) {
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setTenant(MultiTenantContext.getTenant());
        playLaunch.setLaunchId(PlayLaunch.generateLaunchId());
        playLaunch.setUpdatedBy(autoLaunch ? serviceUser : playLaunchChannel.getUpdatedBy());
        playLaunch.setCreatedBy(autoLaunch ? serviceUser : playLaunchChannel.getUpdatedBy());
        playLaunch.setPlay(play);
        playLaunch.setPlayLaunchChannel(playLaunchChannel);
        playLaunch.setLaunchState(state != null ? state : LaunchState.UnLaunched);
        playLaunch.setTopNCount(playLaunchChannel.getMaxAccountsToLaunch());
        playLaunch.setBucketsToLaunch(playLaunchChannel.getBucketsToLaunch());
        playLaunch.setLaunchUnscored(playLaunchChannel.isLaunchUnscored());
        playLaunch.setDestinationOrgId(playLaunchChannel.getLookupIdMap().getOrgId());
        playLaunch.setDestinationSysType(playLaunchChannel.getLookupIdMap().getExternalSystemType());
        playLaunch.setDestinationAccountId(playLaunchChannel.getLookupIdMap().getAccountId());
        playLaunch.setTableName(createTable());
        playLaunchChannel.getChannelConfig().populateLaunchFromChannelConfig(playLaunch);
        playLaunch.setChannelConfig(playLaunchChannel.getChannelConfig());

        Long totalAvailableRatedAccounts = play.getTargetSegment().getAccounts();
        Long totalAvailableContacts = play.getTargetSegment().getContacts();
        playLaunch.setAccountsSelected(totalAvailableRatedAccounts != null ? totalAvailableRatedAccounts : 0L);
        playLaunch.setAccountsSuppressed(0L);
        playLaunch.setAccountsErrored(0L);
        playLaunch.setAccountsLaunched(0L);
        playLaunch.setContactsSelected(totalAvailableContacts != null ? totalAvailableContacts : 0L);
        playLaunch.setContactsLaunched(0L);
        playLaunch.setContactsSuppressed(0L);
        playLaunch.setContactsErrored(0L);
        return playLaunch;
    }

    @Override
    public PlayLaunch queueNewLaunchForChannel(Play play, PlayLaunchChannel playLaunchChannel) {
        return queueNewLaunchForChannel(play, playLaunchChannel, null, null, null, null, null, false);
    }

    private void runValidations(String customerSpace, Play play, PlayLaunchChannel playLaunchChannel) {
        PlayUtils.validatePlay(play);
        validatePlayAndChannelBeforeLaunch(customerSpace, play, playLaunchChannel);
    }

    private List<PlayLaunchChannel> addUnlaunchedChannels(List<PlayLaunchChannel> channels) {
        List<LookupIdMap> allConnections = lookupIdMappingEntityMgr.getLookupIdsMapping(null, null, true);
        if (CollectionUtils.isNotEmpty(allConnections)) {
            allConnections.forEach(mapping -> addToListIfDoesntExist(mapping, channels));
        }
        return channels;
    }

    private void addToListIfDoesntExist(LookupIdMap mapping, List<PlayLaunchChannel> channels) {
        String configId = mapping.getId();
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

        if (channel.getLookupIdMap().getExternalSystemName() == CDLExternalSystemName.Salesforce
                && ((SalesforceChannelConfig) channel.getChannelConfig()).isSuppressAccountsWithoutLookupId()
                && StringUtils.isBlank(channel.getLookupIdMap().getAccountId())) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Cannot restrict accounts with null Ids if account id has not been set up for selected Connection" });
        }

        RatingEnginesCoverageRequest coverageRequest = new RatingEnginesCoverageRequest();
        coverageRequest.setRatingEngineIds(Collections.singletonList(play.getRatingEngine().getId()));
        if (channel.getLookupIdMap().getExternalSystemType() == CDLExternalSystemType.CRM)
            coverageRequest.setRestrictNullLookupId(
                    ((SalesforceChannelConfig) channel.getChannelConfig()).isSuppressAccountsWithoutLookupId());
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
                + (channel.isLaunchUnscored()
                        ? coverageResponse.getRatingModelsCoverageMap().get(play.getRatingEngine().getId())
                                .getUnscoredAccountCount()
                        : 0L);

        if (accountsToLaunch <= 0L) {
            throw new LedpException(LedpCode.LEDP_18176, new String[] { play.getName() });
        }
    }

}
