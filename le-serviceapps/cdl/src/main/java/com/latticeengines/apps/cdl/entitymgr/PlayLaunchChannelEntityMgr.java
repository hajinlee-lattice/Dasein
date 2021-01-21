package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelEntityMgr extends BaseEntityMgrRepository<PlayLaunchChannel, Long> {

    List<PlayLaunchChannel> findByIsAlwaysOnTrue();

    List<PlayLaunchChannel> findByPlayName(String playName);

    void deleteByChannelId(String id, boolean hardDelete);

    PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playName, String lookupId);

    PlayLaunchChannel findById(String channelId);

    PlayLaunchChannel findChannelAndPlayById(String channelId);

    PlayLaunchChannel findById(String channelId, boolean useWriterRepo);

    PlayLaunchChannel updatePlayLaunchChannel(PlayLaunchChannel existingPlayLaunchChannel,
            PlayLaunchChannel playLaunchChannel);

    PlayLaunchChannel createPlayLaunchChannel(PlayLaunchChannel playLaunchChannel);

    List<PlayLaunchChannel> getAllValidScheduledChannels();

    void updateAttributeSetNameToDefault(String attributeSetName);

    PlayLaunchChannel recoverLaunchUniverse(PlayLaunchChannel existingPlayLaunchChannel,
            PlayLaunchChannel updatedChannel);
}
