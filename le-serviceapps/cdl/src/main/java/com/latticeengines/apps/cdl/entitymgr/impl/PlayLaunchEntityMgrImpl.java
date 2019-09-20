package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
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
    @Transactional(propagation = Propagation.REQUIRED)
    public void update(PlayLaunch playLaunch) {
        PlayLaunch existingPlayLaunch = findByLaunchId(playLaunch.getId());
        if (playLaunch.getLaunchState() != null) {
            existingPlayLaunch.setLaunchState(playLaunch.getLaunchState());
        }
        if (StringUtils.isNotBlank(playLaunch.getApplicationId())) {
            existingPlayLaunch.setApplicationId(playLaunch.getApplicationId());
        }
        if (StringUtils.isNotBlank(playLaunch.getTableName())) {
            existingPlayLaunch.setTableName(playLaunch.getTableName());
        }

        // Account stats
        if (playLaunch.getAccountsSelected() != null) {
            existingPlayLaunch.setAccountsSelected(playLaunch.getAccountsSelected());
        }
        if (playLaunch.getAccountsLaunched() != null) {
            existingPlayLaunch.setAccountsLaunched(playLaunch.getAccountsLaunched());
        }
        if (playLaunch.getAccountsSuppressed() != null) {
            existingPlayLaunch.setAccountsSuppressed(playLaunch.getAccountsSuppressed());
        }
        if (playLaunch.getAccountsErrored() != null) {
            existingPlayLaunch.setAccountsErrored(playLaunch.getAccountsErrored());
        }
        if (playLaunch.getAccountsDuplicated() != null) {
            existingPlayLaunch.setAccountsDuplicated(playLaunch.getAccountsDuplicated());
        }

        // Contact stats
        if (playLaunch.getContactsSelected() != null) {
            existingPlayLaunch.setContactsSelected(playLaunch.getContactsSelected());
        }
        if (playLaunch.getContactsLaunched() != null) {
            existingPlayLaunch.setContactsLaunched(playLaunch.getContactsLaunched());
        }
        if (playLaunch.getContactsSuppressed() != null) {
            existingPlayLaunch.setContactsSuppressed(playLaunch.getContactsSuppressed());
        }
        if (playLaunch.getContactsErrored() != null) {
            existingPlayLaunch.setContactsErrored(playLaunch.getContactsErrored());
        }
        if (playLaunch.getContactsDuplicated() != null) {
            existingPlayLaunch.setContactsDuplicated(playLaunch.getContactsDuplicated());
        }

        // Tray System properties
        if (StringUtils.isNotBlank(playLaunch.getAudienceId())) {
            existingPlayLaunch.setAudienceId(playLaunch.getAudienceId());
        }
        if (StringUtils.isNotBlank(playLaunch.getAudienceName())) {
            existingPlayLaunch.setAudienceName(playLaunch.getAudienceName());
        }
        if (playLaunch.getAudienceSize() != null) {
            existingPlayLaunch.setAudienceSize(playLaunch.getAudienceSize());
        }
        if (StringUtils.isNotBlank(playLaunch.getFolderName())) {
            existingPlayLaunch.setFolderName(playLaunch.getFolderName());
        }
        if (playLaunch.getMatchedCount() != null) {
            existingPlayLaunch.setMatchedCount(playLaunch.getMatchedCount());
        }

        // TODO: Keeping here for backwards compatibility with UI, should go away after M32
        if (CollectionUtils.isNotEmpty(playLaunch.getBucketsToLaunch())) {
            existingPlayLaunch.setBucketsToLaunch(playLaunch.getBucketsToLaunch());
        }
        if (StringUtils.isNotBlank(playLaunch.getDestinationAccountId())) {
            existingPlayLaunch.setDestinationAccountId(playLaunch.getDestinationAccountId());
        }
        if (StringUtils.isNotBlank(playLaunch.getDestinationOrgId())) {
            existingPlayLaunch.setDestinationOrgId(playLaunch.getDestinationOrgId());
        }
        if (playLaunch.getDestinationSysType() != null) {
            existingPlayLaunch.setDestinationSysType(playLaunch.getDestinationSysType());
        }
        if (playLaunch.getDestinationOrgName() != null) {
            existingPlayLaunch.setDestinationOrgName(playLaunch.getDestinationOrgName());
        }
        if (playLaunch.getExcludeItemsWithoutSalesforceId() != null) {
            existingPlayLaunch.setExcludeItemsWithoutSalesforceId(playLaunch.getExcludeItemsWithoutSalesforceId());
        }
        existingPlayLaunch.setLaunchUnscored(playLaunch.isLaunchUnscored());

        existingPlayLaunch.setLaunchCompletionPercent(playLaunch.getLaunchCompletionPercent());
        existingPlayLaunch.setUpdatedBy(playLaunch.getUpdatedBy());
        playLaunchDao.update(existingPlayLaunch);
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
    public PlayLaunchChannel findPlayLaunchChannelByLaunchId(String launchId) {
        return playLaunchDao.findPlayLaunchChannelByLaunchId(launchId);
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
    public PlayLaunch findLatestByChannel(Long playLaunchChannelId) {
        return playLaunchDao.findLatestByChannel(playLaunchChannelId);
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
            Hibernate.initialize(launch.getPlayLaunchChannel());
        });
        return launches;
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
                playLaunch.setLaunchState(
                        playLaunch.getLaunchState().isTerminal() ? playLaunch.getLaunchState() : LaunchState.Canceled);
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
