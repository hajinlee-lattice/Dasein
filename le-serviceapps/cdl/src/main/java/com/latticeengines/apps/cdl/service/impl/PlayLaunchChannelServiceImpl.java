package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("playLaunchChannelService")
public class PlayLaunchChannelServiceImpl implements PlayLaunchChannelService {

    private static Logger log = LoggerFactory.getLogger(PlayLaunchChannelServiceImpl.class);

    @Inject
    private PlayService playService;

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

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
            createPlayLaunchFromChannel(playLaunchChannel, play);
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
            createPlayLaunchFromChannel(playLaunchChannel, play);
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
        } else {
            playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrievedPlayLaunchChannel, playLaunchChannel);
        }
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
        if (includeUnlaunchedChannels) {
            addUnlaunchedChannels(channels);
        }
        return channels;
    }

    @Override
    public PlayLaunch createPlayLaunchFromChannel(PlayLaunchChannel playLaunchChannel, Play play) {
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setTenant(MultiTenantContext.getTenant());
        playLaunch.setLaunchId(PlayLaunch.generateLaunchId());
        playLaunch.setUpdatedBy(playLaunchChannel.getUpdatedBy());
        playLaunch.setCreatedBy(playLaunchChannel.getUpdatedBy());
        playLaunch.setPlay(play);
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
        }

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

}
