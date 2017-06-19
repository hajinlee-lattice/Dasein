package com.latticeengines.pls.service;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

public interface PlayLaunchService {

    void create(PlayLaunch entity);

    PlayLaunch findByLaunchId(String launchId);

    void deleteByLaunchId(String launchId);

    PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp);

    List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> launchStates);

    List<PlayLaunch> findByState(LaunchState state);

    PlayLaunch update(PlayLaunch existingPlayLaunch);
}
