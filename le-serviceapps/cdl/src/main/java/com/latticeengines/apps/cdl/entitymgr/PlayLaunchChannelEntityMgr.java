package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelEntityMgr extends BaseEntityMgrRepository<PlayLaunchChannel, Long> {

    List<PlayLaunchChannel> findByIsAlwaysOnTrue();

    PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playName, String lookupId);

    PlayLaunchChannel findById(String channelId);

    PlayLaunchChannel updatePlayLaunchChannel(PlayLaunchChannel playLaunchChannel,
            PlayLaunchChannel existingPlayLaunchChannel);

    PlayLaunchChannel createPlayLaunchChannel(PlayLaunchChannel playLaunchChannel);

}
