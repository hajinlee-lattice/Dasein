package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.LaunchHistory;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayOverview;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.service.PlayLaunchService;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("playService")
public class PlayServiceImpl implements PlayService {

    private static Logger log = LoggerFactory.getLogger(PlayServiceImpl.class);

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    TalkingPointProxy talkingPointProxy;

    @Autowired
    PlayLaunchService playLaunchService;

    @Override
    public Play createOrUpdate(Play play, String tenantId) {
        log.info(String.format("Creating play with name: %s, segment name: %s, on tenant %s", play.getName(),
                play.getSegmentName(), tenantId));
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        play.setTenant(tenant);
        return playEntityMgr.createOrUpdatePlay(play);
    }

    @Override
    public List<Play> getAllPlays() {
        return playEntityMgr.findAll();
    }

    @Override
    public Play getPlayByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        return playEntityMgr.findByName(name);
    }

    @Override
    public List<PlayOverview> getAllPlayOverviews() {
        List<PlayOverview> playOverviews = new ArrayList<PlayOverview>();
        List<String> playNames = getAllPlays().stream().map(p -> p.getName()).collect(Collectors.toList());
        log.info("playNames is " + playNames);
        playNames.stream().forEach(p -> {
            playOverviews.add(getPlayOverviewByName(p));
        });
        return playOverviews;
    }

    @Override
    public PlayOverview getPlayOverviewByName(String name) {
        PlayOverview playOverview = new PlayOverview();
        Play play = getPlayByName(name);
        if (play == null) {
            return null;
        }
        playOverview.setPlay(play);

        ResponseDocument<List<TalkingPointDTO>> response = talkingPointProxy.findAllByPlayName(name);
        playOverview.setTalkingPoints(response.getResult());

        List<LaunchState> launchStates = new ArrayList<>();
        // launchStates.add(LaunchState.Launched);
        PlayLaunch playLaunch = playLaunchService.findLatestByPlayId(play.getPid(), launchStates);
        LaunchHistory launchHistory = new LaunchHistory();
        launchHistory.setLastAccountsNum(playLaunch != null ? playLaunch.getAccountsNum() : null);
        launchHistory.setLastContactsNum(playLaunch != null ? playLaunch.getContactsNum() : null);
        // ----------------------------------------------------------------------------------------------
        // TODO in M14, we will contact Redshift to get new contacts number and
        // accounts number
        // for now, just mock them
        launchHistory.setNewAccountsNum(5000L);
        launchHistory.setNewContactsNum(6000L);
        // ----------------------------------------------------------------------------------------------
        playOverview.setLaunchHistory(launchHistory);

        // ----------------------------------------------------------------------------------------------
        // TODO in M14, we will get real data for AccountRatingMap
        // for now, just mock them
        Map<BucketName, Integer> accountRatingMap = new HashMap<BucketName, Integer>();
        accountRatingMap.put(BucketName.A, 500);
        accountRatingMap.put(BucketName.B, 1000);
        accountRatingMap.put(BucketName.C, 1000);
        accountRatingMap.put(BucketName.D, 1000);
        accountRatingMap.put(BucketName.F, 500);
        // ----------------------------------------------------------------------------------------------
        playOverview.setAccountRatingMap(accountRatingMap);

        return playOverview;
    }

    @Override
    public void deleteByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        playEntityMgr.deleteByName(name);
    }

}
