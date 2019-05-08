package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

@Component("playLaunchChannelService")
public class PlayLaunchChannelServiceImpl implements PlayLaunchChannelService {

    private static Logger log = LoggerFactory.getLogger(PlayLaunchChannelServiceImpl.class);

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Override
    public PlayLaunchChannel create(PlayLaunchChannel playLaunchChannel) {
        return playLaunchChannelEntityMgr.createPlayLaunchChannel(playLaunchChannel);
    }

    @Override
    public PlayLaunchChannel update(PlayLaunchChannel playLaunchChannel) {
        log.info(playLaunchChannel.getPlay().getName());
        log.info(playLaunchChannel.getLookupIdMap().getId());
        log.info(playLaunchChannel.getPlayLaunch().getLaunchId());

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

}
