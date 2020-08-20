package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.pls.service.TeamWrapperService;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component("playService")
public class PlayServiceImpl implements PlayService {

    private static final Logger log = LoggerFactory.getLogger(PlayServiceImpl.class);

    @Inject
    private PlayProxy playProxy;

    @Inject
    private TeamWrapperService teamWrapperService;

    @Override
    public List<Play> getPlays(Boolean shouldLoadCoverage, String ratingEngineId) {
        // by default shouldLoadCoverage flag should be false otherwise play
        // listing API takes lot of time to load
        shouldLoadCoverage = shouldLoadCoverage != null && shouldLoadCoverage;
        Tenant tenant = MultiTenantContext.getTenant();
        List<Play> plays = playProxy.getPlays(tenant.getId(), shouldLoadCoverage, ratingEngineId);
        teamWrapperService.fillTeamInfoForList(plays);
        return plays;
    }

    @Override
    public Play getPlay(String playName) {
        Tenant tenant = MultiTenantContext.getTenant();
        Play play = playProxy.getPlay(tenant.getId(), playName);
        teamWrapperService.fillTeamInfo(play);
        return play;
    }

    @Override
    public Play createOrUpdate(Play play) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.warn("Tenant is null for the request");
            return null;
        }
        if (play == null) {
            throw new NullPointerException("Play is null");
        }

        if (StringUtils.isEmpty(play.getCreatedBy())) {
            play.setCreatedBy(MultiTenantContext.getEmailAddress());
        }
        if (StringUtils.isEmpty(play.getUpdatedBy())) {
            play.setUpdatedBy(MultiTenantContext.getEmailAddress());
        }
        return playProxy.createOrUpdatePlay(tenant.getId(), play);
    }

    @Override
    public void delete(String playName, Boolean hardDelete) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.deletePlay(tenant.getId(), playName, hardDelete);
    }

    @Override
    public List<PlayLaunchChannel> getPlayLaunchChannels(String playName, Boolean includeUnlaunchedChannels) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunchChannels(tenant.getId(), playName, includeUnlaunchedChannels);
    }

    @Override
    public PlayLaunchChannel createPlayLaunchChannel(String playName, PlayLaunchChannel playLaunchChannel,
            Boolean launchNow) {
        Tenant tenant = MultiTenantContext.getTenant();
        playLaunchChannel.setCreatedBy(MultiTenantContext.getEmailAddress());
        playLaunchChannel.setUpdatedBy(MultiTenantContext.getEmailAddress());
        return playProxy.createPlayLaunchChannel(tenant.getId(), playName, playLaunchChannel, launchNow);
    }

    @Override
    public PlayLaunchChannel updatePlayLaunchChannel(String playName, String channelId,
            PlayLaunchChannel playLaunchChannel, Boolean launchNow) {
        Tenant tenant = MultiTenantContext.getTenant();
        playLaunchChannel.setUpdatedBy(MultiTenantContext.getEmailAddress());
        return playProxy.updatePlayLaunchChannel(tenant.getId(), playName, channelId, playLaunchChannel, launchNow);
    }

    @Override
    public PlayLaunchDashboard getPlayLaunchDashboard(String playName, String orgId, String externalSysType,
            List<LaunchState> launchStates, Long startTimestamp, Long offset, Long max, String sortBy,
            boolean descending, Long endTimestamp) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunchDashboard(tenant.getId(), playName, launchStates, startTimestamp, offset, max,
                sortBy, descending, endTimestamp, orgId, externalSysType);
    }

    @Override
    public Long getPlayLaunchDashboardEntriesCount(String playName, String orgId, String externalSysType,
            List<LaunchState> launchStates, Long startTimestamp, Long endTimestamp) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunchDashboardEntriesCount(tenant.getId(), playName, launchStates, startTimestamp,
                endTimestamp, orgId, externalSysType);
    }

    @Override
    public PlayLaunch createPlayLaunch(String playName, PlayLaunch playLaunch) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (StringUtils.isEmpty(playLaunch.getCreatedBy())) {
            playLaunch.setCreatedBy(MultiTenantContext.getEmailAddress());
        }
        if (StringUtils.isEmpty(playLaunch.getUpdatedBy())) {
            playLaunch.setUpdatedBy(MultiTenantContext.getEmailAddress());
        }
        // TODO: Clean up needed by Perry
        // return playProxy.createPlayLaunch(tenant.getId(), playName, playLaunch);
        return null;
    }

    @Override
    public PlayLaunch updatePlayLaunch(String playName, String launchId, PlayLaunch playLaunch) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (StringUtils.isEmpty(playLaunch.getCreatedBy())) {
            playLaunch.setCreatedBy(MultiTenantContext.getEmailAddress());
        }
        if (StringUtils.isEmpty(playLaunch.getUpdatedBy())) {
            playLaunch.setUpdatedBy(MultiTenantContext.getEmailAddress());
        }
        return playProxy.updatePlayLaunch(tenant.getId(), playName, launchId, playLaunch);
    }

    @Override
    public PlayLaunch launchPlay(String playName, String launchId) {
        Tenant tenant = MultiTenantContext.getTenant();
        // TODO: Clean up needed by Perry
        // return playProxy.launchPlay(tenant.getId(), playName, launchId, false);
        return null;
    }

    @Override
    public List<PlayLaunch> getPlayLaunches(String playName, List<LaunchState> launchStates) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunches(tenant.getId(), playName, launchStates);
    }

    @Override
    public PlayLaunch getPlayLaunch(String playName, String launchId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunch(tenant.getId(), playName, launchId);
    }

    @Override
    public PlayLaunch updatePlayLaunch(String playName, String launchId, LaunchState action) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.updatePlayLaunch(tenant.getId(), playName, launchId, action);
        return playProxy.getPlayLaunch(tenant.getId(), playName, launchId);
    }

    @Override
    public void deletePlayLaunch(String playName, String launchId, Boolean hardDelete) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.deletePlayLaunch(tenant.getId(), playName, launchId, hardDelete);
    }

}
