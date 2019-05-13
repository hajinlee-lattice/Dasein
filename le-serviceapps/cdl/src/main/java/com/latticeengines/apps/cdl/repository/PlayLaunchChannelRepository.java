package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelRepository extends BaseJpaRepository<PlayLaunchChannel, Long> {

    PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playId, String lookupId);

    PlayLaunchChannel findById(String channelId);

    List<PlayLaunchChannel> findByIsAlwaysOnTrue();
}
