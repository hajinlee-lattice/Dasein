package com.latticeengines.pls.dao;

import java.util.Date;
import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

public interface PlayLaunchDao extends BaseDao<PlayLaunch> {

    PlayLaunch findByLaunchId(String launchId);

    PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp);

    List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states);

    List<PlayLaunch> findByState(LaunchState state);

}
