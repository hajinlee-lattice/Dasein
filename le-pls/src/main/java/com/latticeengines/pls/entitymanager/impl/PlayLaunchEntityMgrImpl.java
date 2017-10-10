package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.PlayLaunchDao;
import com.latticeengines.pls.entitymanager.PlayLaunchEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("playLaunchEntityMgr")
public class PlayLaunchEntityMgrImpl extends BaseEntityMgrImpl<PlayLaunch> implements PlayLaunchEntityMgr {

    private static final String PLAY_LAUNCH_NAME_PREFIX = "launch";
    private static final String PLAY_LAUNCH_NAME_FORMAT = "%s__%s";

    @Autowired
    private PlayLaunchDao playLaunchDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<PlayLaunch> getDao() {
        return playLaunchDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(PlayLaunch entity) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        entity.setTenant(tenant);
        entity.setLaunchId(generateLaunchId());
        playLaunchDao.create(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunch> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayLaunch findByLaunchId(String launchId) {
        return playLaunchDao.findByLaunchId(launchId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp) {
        return playLaunchDao.findByPlayAndTimestamp(playId, timestamp);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states) {
        return playLaunchDao.findByPlayId(playId, states);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayLaunch findLatestByPlayId(Long playId, List<LaunchState> states) {
        return playLaunchDao.findLatestByPlayId(playId, states);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunch> findByState(LaunchState state) {
        return playLaunchDao.findByState(state);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByLaunchId(String launchId) {
        PlayLaunch playLaunch = findByLaunchId(launchId);
        deletePlayLaunch(playLaunch);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunch> findDashboardEntries(Long playId, List<LaunchState> states, Long startTimestamp,
            Long offset, Long max, Long endTimestamp) {
        return playLaunchDao.findByPlayStatesAndPagination(playId, states, startTimestamp, offset, max, endTimestamp);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long findDashboardEntriesCount(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp) {
        return playLaunchDao.findCountByPlayStatesAndTimestamps(playId, states, startTimestamp, endTimestamp);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Stats findDashboardCumulativeStats(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp) {
        return playLaunchDao.findTotalCountByPlayStatesAndTimestamps(playId, states, startTimestamp, endTimestamp);
    }

    private void deletePlayLaunch(PlayLaunch playLaunch) {
        if (playLaunch != null) {
            playLaunchDao.delete(playLaunch);
        }
    }

    private String generateLaunchId() {
        return String.format(PLAY_LAUNCH_NAME_FORMAT, PLAY_LAUNCH_NAME_PREFIX, UUID.randomUUID().toString());
    }
}
