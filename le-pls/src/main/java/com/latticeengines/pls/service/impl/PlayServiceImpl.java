package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.LaunchHistory;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketCoverage;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.service.PlayLaunchService;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("playService")
public class PlayServiceImpl implements PlayService {

    private static Logger log = LoggerFactory.getLogger(PlayServiceImpl.class);

    @Value("${pls.play.service.threadpool.size:10}")
    private Integer fetcherNum;

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    TalkingPointProxy talkingPointProxy;

    @Autowired
    PlayLaunchService playLaunchService;

    @Autowired
    RatingCoverageService ratingCoverageService;

    private ForkJoinPool tpForParallelStream;

    @PostConstruct
    public void init() {
        tpForParallelStream = ThreadPoolUtils.getForkJoinThreadPool("play-details-fetcher", fetcherNum);
    }

    @Override
    public Play createOrUpdate(Play play, String tenantId) {
        log.info(String.format("Creating play with name: %s, on tenant %s", play.getName(), tenantId));
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        play.setTenant(tenant);
        Play retrievedPlay = playEntityMgr.createOrUpdatePlay(play);
        return getFullPlay(retrievedPlay, true, null);
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
    public List<Play> getAllFullPlays(boolean shouldLoadCoverage) {
        List<Play> plays = getAllPlays();
        Tenant tenant = MultiTenantContext.getTenant();

        // to make loading of play list efficient and faster we need to run full
        // play loading in parallel
        tpForParallelStream.submit(//
                () -> //
                plays //
                        .stream().parallel() //
                        .forEach( //
                                play -> //
                                getFullPlay(play, shouldLoadCoverage, tenant))) //
                .join();

        return plays;
    }

    @Override
    public Play getFullPlayByName(String name) {
        Play play = getPlayByName(name);
        return getFullPlay(play, true, null);
    }

    private Play getFullPlay(Play play, boolean shouldLoadCoverage, Tenant tenant) {
        if (play == null) {
            return null;
        }
        if (tenant != null) {
            MultiTenantContext.setTenant(tenant);
        }

        List<LaunchState> launchStates = new ArrayList<>();
        launchStates.add(LaunchState.Launched);
        PlayLaunch playLaunch = playLaunchService.findLatestByPlayId(play.getPid(), launchStates);
        PlayLaunch mostRecentPlayLaunch = playLaunchService.findLatestByPlayId(play.getPid(), null);
        LaunchHistory launchHistory = new LaunchHistory();
        launchHistory.setPlayLaunch(playLaunch);
        launchHistory.setMostRecentLaunch(mostRecentPlayLaunch);
        play.setLaunchHistory(launchHistory);

        RatingEngine ratingEngine = play.getRatingEngine();
        if (ratingEngine == null || ratingEngine.getId() == null) {
            log.info(String.format("Rating Engine for Play %s is not defined.", play.getName()));
        } else {
            if (shouldLoadCoverage) {
                populateCoverageInfo(play, playLaunch, launchHistory, ratingEngine);
            }
        }

        return play;
    }

    private void populateCoverageInfo(Play play, PlayLaunch playLaunch, LaunchHistory launchHistory,
            RatingEngine ratingEngine) {
        try {
            RatingsCountRequest request = new RatingsCountRequest();
            request.setRatingEngineIds(Arrays.asList(ratingEngine.getId()));
            RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request);
            Map<String, CoverageInfo> ratingEngineIdCoverageMap = response.getRatingEngineIdCoverageMap();
            Long accountCount = ratingEngineIdCoverageMap.get(ratingEngine.getId()).getAccountCount() == null ? 0L
                    : ratingEngineIdCoverageMap.get(ratingEngine.getId()).getAccountCount();
            Long contactCount = ratingEngineIdCoverageMap.get(ratingEngine.getId()).getContactCount() == null ? 0L
                    : ratingEngineIdCoverageMap.get(ratingEngine.getId()).getContactCount();
            List<RatingBucketCoverage> ratingBucketCoverage = ratingEngineIdCoverageMap.get(ratingEngine.getId())
                    .getBucketCoverageCounts();
            play.setRatings(ratingBucketCoverage);

            log.info(String.format("For play %s, new account number and contact number are %d and %d, respectively",
                    play.getName(), accountCount, contactCount));

            Long mostRecentSucessfulLaunchAccountNum = playLaunch == null ? 0l : playLaunch.getAccountsLaunched();
            Long mostRecentSucessfulLaunchContactNum = playLaunch == null ? 0l : playLaunch.getContactsLaunched();

            Long newAccountsNum = mostRecentSucessfulLaunchAccountNum == null ? accountCount
                    : accountCount - mostRecentSucessfulLaunchAccountNum;
            Long newContactsNum = mostRecentSucessfulLaunchContactNum == null ? contactCount
                    : contactCount - mostRecentSucessfulLaunchContactNum;

            launchHistory.setNewAccountsNum(newAccountsNum);
            launchHistory.setNewContactsNum(newContactsNum);
        } catch (Exception ex) {
            // any exception in getting count info should not block
            // rendering of play listing
            log.info("Ignoring exception in get cont info", ex);
        }
    }

    @Override
    public void deleteByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        playEntityMgr.deleteByName(name);
    }

    @Override
    public void publishTalkingPoints(String playName, String customerSpace) {
        if (StringUtils.isBlank(playName)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        Play play = playEntityMgr.findByName(playName);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_18144, new String[] { playName });
        }

        talkingPointProxy.publish(playName, customerSpace);
        play.setLastTalkingPointPublishTime(new Date());
        playEntityMgr.update(play);
    }

}
