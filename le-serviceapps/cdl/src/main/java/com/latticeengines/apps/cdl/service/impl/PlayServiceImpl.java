package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.LaunchHistory;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.RatingEngineUtils;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;

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
        MultiTenantContext.setTenant(tenant);
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
        Tenant tenant = MultiTenantContext.getTenant();
        List<Play> plays = null;
        if (ratingEngineId == null) {
            plays = getAllPlays();
        } else {
            RatingEngine ratingEngine = validateRatingEngineId(tenant, ratingEngineId);
            plays = playEntityMgr.findAllByRatingEnginePid(ratingEngine.getPid());
        }

        final List<Play> innerPlays = new ArrayList<>();
        innerPlays.addAll(plays);

        Date lastRefreshedDate = findLastRefreshedDate();
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

    private RatingEngine validateRatingEngineId(Tenant tenant, String ratingEngineId) {
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false, false);
        if (ratingEngine == null || ratingEngine.getPid() == null) {
            throw new NullPointerException(
                    String.format("Rating Engine %s has not been fully specified", ratingEngine));
        }
        return ratingEngine;
    }

    @Override
    public Play getFullPlayByName(String name) {
        Play play = getPlayByName(name);
        if (play != null) {
            play = getFullPlay(play, true, null);
        }
        return play;
    }

    private Play getFullPlay(Play play, boolean shouldLoadCoverage, Tenant tenant) {
        if (play != null) {
            Date lastRefreshedDate = findLastRefreshedDate();
            play = getFullPlay(play, shouldLoadCoverage, tenant, lastRefreshedDate);
        }
        return play;
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

            CoverageInfo coverageInfo = RatingEngineUtils.getCoverageInfo(ratingEngine);
            Long accountCount = coverageInfo.getAccountCount();
            if (accountCount == null) {
                throw new IllegalStateException("Cannot find a valid account count from rating engine " + ratingEngine.getId());
            }
            Long contactCount = coverageInfo.getContactCount();
            if (contactCount == null) {
                throw new IllegalStateException("Cannot find a valid contact count from rating engine " + ratingEngine.getId());
            }
            play.setRatings(coverageInfo.getBucketCoverageCounts());

            log.info(String.format("For play %s, account number and contact number are %d and %d, respectively",
                    play.getName(), accountCount, contactCount));

            Long mostRecentSucessfulLaunchAccountNum = mostRecentSuccessfulPlayLaunch == null ? 0L
                    : mostRecentSuccessfulPlayLaunch.getAccountsLaunched();
            Long mostRecentSucessfulLaunchContactNum = mostRecentSuccessfulPlayLaunch == null ? 0L
                    : mostRecentSuccessfulPlayLaunch.getContactsLaunched();

            Long newAccountsNum = accountCount - mostRecentSucessfulLaunchAccountNum;
            Long newContactsNum = contactCount - mostRecentSucessfulLaunchContactNum;

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
