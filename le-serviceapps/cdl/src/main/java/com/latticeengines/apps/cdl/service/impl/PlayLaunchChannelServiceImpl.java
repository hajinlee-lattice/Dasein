package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

@Component("playLaunchChannelService")
public class PlayLaunchChannelServiceImpl implements PlayLaunchChannelService {

    private static Logger log = LoggerFactory.getLogger(PlayLaunchChannelServiceImpl.class);

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

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
            retrievedPlayLaunchChannel = playLaunchChannelEntityMgr.updatePlayLaunchChannel(playLaunchChannel,
                    retrievedPlayLaunchChannel);
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
        List<PlayLaunchChannel> channelList = playLaunchChannelEntityMgr.findAll();
        if (includeUnlaunchedChannels) {
            channelList = createChannelListWithUnlaunchedChannels(channelList);
        }

        return channelList;
    }

    private List<PlayLaunchChannel> createChannelListWithUnlaunchedChannels(List<PlayLaunchChannel> channelList) {
        Map<String, List<LookupIdMap>> allLookupIdMapping = lookupIdMappingEntityMgr.getLookupIdsMapping(null, null,
                true);
        if (MapUtils.isNotEmpty(allLookupIdMapping)) {
            allLookupIdMapping.keySet().stream() //
                    .filter(k -> CollectionUtils.isNotEmpty(allLookupIdMapping.get(k))) //
                    .forEach(k -> allLookupIdMapping.get(k).stream().forEach(mapping -> {
                        addToListIfDoesntExist(mapping, channelList);
                    }));
        }
        return channelList;
    }

    private void addToListIfDoesntExist(LookupIdMap mapping, List<PlayLaunchChannel> channelList) {
        String configId = mapping.getId();
        for (PlayLaunchChannel channel : channelList) {
            if (channel.getLookupIdMap().getId().equals(configId)) {
                return;
            }
        }
        PlayLaunchChannel newChannel = new PlayLaunchChannel();
        newChannel.setLookupIdMap(mapping);
        return;
    }

}
