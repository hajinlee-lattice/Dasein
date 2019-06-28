package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.Hibernate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.PlayLaunchDao;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LaunchSummary;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;

@Component("playLaunchEntityMgr")
public class PlayLaunchEntityMgrImpl extends BaseEntityMgrImpl<PlayLaunch> implements PlayLaunchEntityMgr {

    @Autowired
    private PlayLaunchDao playLaunchDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Override
    public BaseDao<PlayLaunch> getDao() {
        return playLaunchDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(PlayLaunch entity) {
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
    public PlayLaunch findLatestByPlayAndSysOrg(Long playId, String orgId) {
        return playLaunchDao.findLatestByPlayAndSysOrg(playId, orgId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunch> findByState(LaunchState state) {
        return playLaunchDao.findByState(state);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunch> getByStateAcrossTenants(LaunchState state, Long max) {
        List<PlayLaunch> launches = playLaunchDao.getByStateAcrossTenants(state, max);

        launches.forEach(launch -> {
            Hibernate.initialize(launch.getPlay());
        });
        return launches;
    }

    @Override
    public PlayLaunch updatePlayLaunchState(PlayLaunch playLaunch, String appId, LaunchState launchState) {
        return playLaunchDao.updatePlayLaunchState(playLaunch, appId, launchState);
    }

    @Override
    @Modifying
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByLaunchId(String launchId, boolean hardDelete) {
        PlayLaunch playLaunch = findByLaunchId(launchId);
        if (playLaunch != null) {
            if (hardDelete) {
                playLaunchDao.delete(playLaunch);
            } else {
                playLaunch.setDeleted(true);
                playLaunchDao.update(playLaunch);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<LaunchSummary> findDashboardEntries(Long playId, List<LaunchState> states, Long startTimestamp,
            Long offset, Long max, String sortby, boolean descending, Long endTimestamp, String orgId,
            String externalSysType) {
        return playLaunchDao.findByPlayStatesAndPagination(playId, states, startTimestamp, offset, max, sortby,
                descending, endTimestamp, orgId, externalSysType);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long findDashboardEntriesCount(Long playId, List<LaunchState> states, Long startTimestamp, Long endTimestamp,
            String orgId, String externalSysType) {
        return playLaunchDao.findCountByPlayStatesAndTimestamps(playId, states, startTimestamp, endTimestamp, orgId,
                externalSysType);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Play> findDashboardPlaysWithLaunches(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType) {
        return playLaunchDao.findDashboardPlaysWithLaunches(playId, states, startTimestamp, endTimestamp, orgId,
                externalSysType);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Pair<String, String>> findDashboardOrgIdWithLaunches(Long playId, List<LaunchState> states,
            Long startTimestamp, Long endTimestamp, String orgId, String externalSysType) {
        return playLaunchDao.findDashboardOrgIdWithLaunches(playId, states, startTimestamp, endTimestamp, orgId,
                externalSysType);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Stats findDashboardCumulativeStats(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType) {
        return playLaunchDao.findTotalCountByPlayStatesAndTimestamps(playId, states, startTimestamp, endTimestamp,
                orgId, externalSysType);
    }

}
