package com.latticeengines.pls.entitymanager;

import java.util.Date;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

public interface PlayLaunchEntityMgr extends BaseEntityMgr<PlayLaunch> {

    void create(PlayLaunch entity);

    PlayLaunch findByLaunchId(String launchId);

    void deleteByLaunchId(String launchId);

    PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp);

    List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states);

    List<PlayLaunch> findByState(LaunchState state);

}
