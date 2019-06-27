package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.EntityGraph.EntityGraphType;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelRepository extends BaseJpaRepository<PlayLaunchChannel, Long> {

    List<PlayLaunchChannel> findByPlayName(String playName);

    PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playId, String lookupId);

    @EntityGraph(value = "PlayLaunchChannel.play", type = EntityGraphType.LOAD)
    PlayLaunchChannel findById(String channelId);

    @EntityGraph(value = "PlayLaunchChannel.play", type = EntityGraphType.LOAD)
    List<PlayLaunchChannel> findByIsAlwaysOnTrue();
}
