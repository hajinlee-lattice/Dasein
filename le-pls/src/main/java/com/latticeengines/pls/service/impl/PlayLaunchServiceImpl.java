package com.latticeengines.pls.service.impl;

import java.util.Date;
import java.util.List;

import org.codehaus.plexus.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.pls.entitymanager.PlayLaunchEntityMgr;
import com.latticeengines.pls.service.PlayLaunchService;

@Component("playLaunchService")
public class PlayLaunchServiceImpl implements PlayLaunchService {
    @Autowired
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Override
    public void create(PlayLaunch entity) {
        playLaunchEntityMgr.create(entity);
    }

    @Override
    public PlayLaunch findByLaunchId(String launchId) {
        if (StringUtils.isBlank(launchId)) {
            throw new LedpException(LedpCode.LEDP_18146);
        }
        return playLaunchEntityMgr.findByLaunchId(launchId);
    }

    @Override
    public void deleteByLaunchId(String launchId) {
        if (StringUtils.isBlank(launchId)) {
            throw new LedpException(LedpCode.LEDP_18146);
        }
        playLaunchEntityMgr.deleteByLaunchId(launchId);
    }

    @Override
    public PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp) {
        return playLaunchEntityMgr.findByPlayAndTimestamp(playId, timestamp);
    }

    @Override
    public List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states) {
        return playLaunchEntityMgr.findByPlayId(playId, states);
    }

    @Override
    public List<PlayLaunch> findByState(LaunchState state) {
        return playLaunchEntityMgr.findByState(state);
    }

    @Override
    public PlayLaunch update(PlayLaunch playLaunch) {
        playLaunchEntityMgr.update(playLaunch);
        return playLaunchEntityMgr.findByKey(playLaunch);
    }

}
