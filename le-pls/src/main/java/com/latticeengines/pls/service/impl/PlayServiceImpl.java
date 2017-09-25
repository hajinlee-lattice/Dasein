package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
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
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
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
    private TalkingPointProxy talkingPointProxy;

    @Autowired
    private PlayLaunchService playLaunchService;

    @Autowired
    private RatingCoverageService ratingCoverageService;

    @Autowired
    private RatingEngineService ratingEngineService;

    @Autowired
    private DataFeedProxy dataFeedProxy;

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
        List<Play> plays = playEntityMgr.findAll();
        updateLastRefreshedDate(plays);
        return plays;
    }

    @Override
    public Play getPlayByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        Play play = playEntityMgr.findByName(name);
        if (play != null) {
            updateLastRefreshedDate(play.getRatingEngine());
        }
        return play;
    }

    @Override
    public List<Play> getAllFullPlays(boolean shouldLoadCoverage, String ratingEngineId) {
        List<Play> plays = null;
        if (ratingEngineId == null) {
            plays = getAllPlays();
        } else {
            RatingEngine ratingEngine = validateRatingEngineId(ratingEngineId);
            plays = playEntityMgr.findAllByRatingEnginePid(ratingEngine.getPid());
        }

        final List<Play> innerPlays = new ArrayList<>();
        innerPlays.addAll(plays);

        Date lastRefreshedDate = findLastRefreshedDate();
        Tenant tenant = MultiTenantContext.getTenant();
        // to make loading of play list efficient and faster we need to run full
        // play loading in parallel
        tpForParallelStream.submit(//
                () -> //
                innerPlays //
                        .stream() //
                        .parallel() //
                        .forEach( //
                                play -> //
                                getFullPlay(play, shouldLoadCoverage, tenant, lastRefreshedDate))) //
                .join();

        return innerPlays;
    }

    private RatingEngine validateRatingEngineId(String ratingEngineId) {
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false);
        if (ratingEngine == null || ratingEngine.getPid() == null) {
            throw new NullPointerException(
                    String.format("Rating Engine %s has not been fully specified", ratingEngine));
        }
        return ratingEngine;
    }

    @Override
    public Play getFullPlayByName(String name) {
        Play play = getPlayByName(name);
        return getFullPlay(play, true, null);
    }

    private Play getFullPlay(Play play, boolean shouldLoadCoverage, Tenant tenant) {
        Date lastRefreshedDate = findLastRefreshedDate();
        return getFullPlay(play, shouldLoadCoverage, tenant, lastRefreshedDate);
    }

    private Play getFullPlay(Play play, boolean shouldLoadCoverage, Tenant tenant, Date lastRefreshedDate) {
        if (play == null) {
            return null;
        }
        if (tenant != null) {
            MultiTenantContext.setTenant(tenant);
        }

        LaunchHistory launchHistory = updatePlayLaunchHistory(play);

        RatingEngine ratingEngine = play.getRatingEngine();
        if (ratingEngine == null || ratingEngine.getId() == null) {
            log.info(String.format("Rating Engine for Play %s is not defined.", play.getName()));
        } else {
            if (shouldLoadCoverage) {
                populateCoverageInfo(play, launchHistory, ratingEngine);
            }
        }

        updateLastRefreshedDate(play.getRatingEngine(), lastRefreshedDate);
        return play;
    }

    private LaunchHistory updatePlayLaunchHistory(Play play) {
        LaunchHistory launchHistory = getLaunchHistoryForPlay(play);
        play.setLaunchHistory(launchHistory);
        return launchHistory;
    }

    private LaunchHistory getLaunchHistoryForPlay(Play play) {
        List<LaunchState> launchStates = new ArrayList<>();
        launchStates.add(LaunchState.Launched);
        PlayLaunch playLaunch = playLaunchService.findLatestByPlayId(play.getPid(), launchStates);
        PlayLaunch mostRecentPlayLaunch = playLaunchService.findLatestByPlayId(play.getPid(), null);
        LaunchHistory launchHistory = new LaunchHistory();
        launchHistory.setPlayLaunch(playLaunch);
        launchHistory.setMostRecentLaunch(mostRecentPlayLaunch);
        return launchHistory;
    }

    private void populateCoverageInfo(Play play, LaunchHistory launchHistory, RatingEngine ratingEngine) {
        try {
            PlayLaunch mostRecentSuccessfulPlayLaunch = launchHistory.getPlayLaunch();
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

            Long mostRecentSucessfulLaunchAccountNum = mostRecentSuccessfulPlayLaunch == null ? 0l
                    : mostRecentSuccessfulPlayLaunch.getAccountsLaunched();
            Long mostRecentSucessfulLaunchContactNum = mostRecentSuccessfulPlayLaunch == null ? 0l
                    : mostRecentSuccessfulPlayLaunch.getContactsLaunched();

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

    private void updateLastRefreshedDate(RatingEngine ratingEngine) {
        Date lastRefreshedDate = findLastRefreshedDate();
        updateLastRefreshedDate(ratingEngine, lastRefreshedDate);
    }

    private void updateLastRefreshedDate(RatingEngine ratingEngine, Date lastRefreshedDate) {
        if (ratingEngine != null) {
            ratingEngine.setLastRefreshedDate(lastRefreshedDate);
        }
    }

    private void updateLastRefreshedDate(List<Play> plays) {
        if (CollectionUtils.isNotEmpty(plays)) {
            Date lastRefreshedDate = findLastRefreshedDate();
            plays //
                    .stream().forEach(play -> {
                        if (play.getRatingEngine() != null) {
                            play.getRatingEngine().setLastRefreshedDate(lastRefreshedDate);
                        }
                    });
        }
    }

    private Date findLastRefreshedDate() {
        Tenant tenant = MultiTenantContext.getTenant();
        DataFeed dataFeed = dataFeedProxy.getDataFeed(tenant.getId());
        return dataFeed.getLastPublished();
    }
}
