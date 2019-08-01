package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
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
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.ratings.coverage.RatingBucketCoverage;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.PlayUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("playLaunchChannelService")
public class PlayLaunchChannelServiceImpl implements PlayLaunchChannelService {

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

    @Override
    public PlayLaunchChannel createPlayLaunchChannel(String playName, PlayLaunchChannel playLaunchChannel,
            Boolean launchNow) {
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playName });
        }
        playLaunchChannel.setPlay(play);
        playLaunchChannel.setTenant(MultiTenantContext.getTenant());
        playLaunchChannel.setTenantId(MultiTenantContext.getTenant().getPid());
        create(playLaunchChannel);
        if (launchNow) {
            playLaunchChannel.setLastLaunch(createPlayLaunchFromChannel(playLaunchChannel, play));
        }
        return playLaunchChannel;
    }

    @Override
    public PlayLaunchChannel updatePlayLaunchChannel(String playName, PlayLaunchChannel playLaunchChannel,
            Boolean launchNow) {
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playName });
        }
        playLaunchChannel.setPlay(play);
        playLaunchChannel = update(playLaunchChannel);
        if (launchNow) {
            playLaunchChannel.setLastLaunch(createPlayLaunchFromChannel(playLaunchChannel, play));
        }
        return playLaunchChannel;
    }

    @Override
    public PlayLaunchChannel create(PlayLaunchChannel playLaunchChannel) {
        return playLaunchChannelEntityMgr.createPlayLaunchChannel(playLaunchChannel);
    }

    @Override
    public PlayLaunchChannel update(PlayLaunchChannel playLaunchChannel) {
        PlayLaunchChannel retrievedPlayLaunchChannel = findById(playLaunchChannel.getId());
        if (retrievedPlayLaunchChannel == null) {
            throw new NullPointerException("Cannot find Play Launch Channel for given play channel id");
        }
        if (!retrievedPlayLaunchChannel.getPlay().getName().equals(playLaunchChannel.getPlay().getName())) {
            throw new LedpException(LedpCode.LEDP_18225, new String[] { retrievedPlayLaunchChannel.getPlay().getName(),
                    playLaunchChannel.getPlay().getName() });
        }
        playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrievedPlayLaunchChannel, playLaunchChannel);

        return retrievedPlayLaunchChannel;
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
        playLaunchEntityMgr.deleteByLaunchId(channelId, hardDelete);
    }

    @SuppressWarnings("checkstyle:methodlength")
    @Override
    public PlayLaunch createPlayLaunchFromChannel(PlayLaunchChannel playLaunchChannel, Play play) {
        runValidations(MultiTenantContext.getTenant().getId(), play, playLaunchChannel);

        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setTenant(MultiTenantContext.getTenant());
        playLaunch.setLaunchId(PlayLaunch.generateLaunchId());
        playLaunch.setUpdatedBy(playLaunchChannel.getUpdatedBy());
        playLaunch.setCreatedBy(playLaunchChannel.getUpdatedBy());
        playLaunch.setPlay(play);
        playLaunch.setPlayLaunchChannel(playLaunchChannel);
        playLaunch.setLaunchState(LaunchState.Queued);
        playLaunch.setTopNCount(playLaunchChannel.getMaxAccountsToLaunch());
        playLaunch.setBucketsToLaunch(playLaunchChannel.getBucketsToLaunch());
        playLaunch.setLaunchUnscored(playLaunchChannel.isLaunchUnscored());
        playLaunch.setDestinationOrgId(playLaunchChannel.getLookupIdMap().getOrgId());
        playLaunch.setDestinationSysType(playLaunchChannel.getLookupIdMap().getExternalSystemType());
        playLaunch.setDestinationAccountId(playLaunchChannel.getLookupIdMap().getAccountId());
        playLaunch.setTableName(createTable());

        if (playLaunchChannel.getChannelConfig() instanceof MarketoChannelConfig) {
            MarketoChannelConfig channelConfig = (MarketoChannelConfig) playLaunchChannel.getChannelConfig();
            playLaunch.setAudienceId(channelConfig.getAudienceId());
            playLaunch.setAudienceName(channelConfig.getAudienceName());
            playLaunch.setFolderName(channelConfig.getFolderName());
        } else if (playLaunchChannel.getChannelConfig() instanceof SalesforceChannelConfig) {
            SalesforceChannelConfig channelConfig = (SalesforceChannelConfig) playLaunchChannel.getChannelConfig();
            playLaunch.setExcludeItemsWithoutSalesforceId(channelConfig.isSupressAccountWithoutAccountId());
        } else if (playLaunchChannel.getChannelConfig() instanceof LinkedInChannelConfig) {
            LinkedInChannelConfig channelConfig = (LinkedInChannelConfig) playLaunchChannel.getChannelConfig();
            playLaunch.setAudienceId(channelConfig.getAudienceId());
            playLaunch.setAudienceName(channelConfig.getAudienceName());
            channelConfig.setAudienceType(channelConfig.getAudienceType());
        } else if (playLaunchChannel.getChannelConfig() instanceof FacebookChannelConfig) {
            FacebookChannelConfig channelConfig = (FacebookChannelConfig) playLaunchChannel.getChannelConfig();
            playLaunch.setAudienceId(channelConfig.getAudienceId());
            playLaunch.setAudienceName(channelConfig.getAudienceName());
            channelConfig.setAudienceType(channelConfig.getAudienceType());
        }
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

        playLaunchEntityMgr.create(playLaunch);
        return playLaunch;
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
        // TODO: Remove when Launch to S3 is GA
        boolean enableS3 = batonService.isEnabled(MultiTenantContext.getCustomerSpace(),
                LatticeFeatureFlag.ALPHA_FEATURE);
        if (mapping.getExternalSystemName() == null
                || (mapping.getExternalSystemName().equals(CDLExternalSystemName.AWS_S3) && !enableS3)) {
            return;
        }
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

        if (channel.getLookupIdMap().getExternalSystemType() == CDLExternalSystemType.CRM
                && ((SalesforceChannelConfig) channel.getChannelConfig()).isSupressAccountWithoutAccountId()
                && StringUtils.isBlank(channel.getLookupIdMap().getAccountId())) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Cannot restrict accounts with null Ids if account id has not been set up for selected Connection" });
        }

        RatingEnginesCoverageRequest coverageRequest = new RatingEnginesCoverageRequest();
        coverageRequest.setRatingEngineIds(Collections.singletonList(play.getRatingEngine().getId()));
        if (channel.getLookupIdMap().getExternalSystemType() == CDLExternalSystemType.CRM)
            coverageRequest.setRestrictNullLookupId(
                    ((SalesforceChannelConfig) channel.getChannelConfig()).isSupressAccountWithoutAccountId());
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
