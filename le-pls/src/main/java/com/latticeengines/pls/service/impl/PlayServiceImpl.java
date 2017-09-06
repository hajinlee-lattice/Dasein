package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.BucketInformation;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.LaunchHistory;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketCoverage;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingObject;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.service.PlayLaunchService;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.pls.service.RatingCoverageService;
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

    @Autowired
    RatingCoverageService ratingCoverageService;

    @Override
    public Play createOrUpdate(Play play, String tenantId) {
        log.info(String.format("Creating play with name: %s, segment name: %s, on tenant %s", play.getName(),
                play.getSegmentName(), tenantId));
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        play.setTenant(tenant);
        Play retrievedPlay = playEntityMgr.createOrUpdatePlay(play);
        return getFullPlay(retrievedPlay);
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
    public List<Play> getAllFullPlays() {
        return getAllPlays().stream().map(this::getFullPlay).collect(Collectors.toList());
    }

    @Override
    public Play getFullPlayByName(String name) {
        Play play = getPlayByName(name);
        return getFullPlay(play);
    }

    private Play getFullPlay(Play play) {
        if (play == null) {
            return null;
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
            // TODO comment this for now, will enable this after play is fully
            // integrated with Rating Engine.
            // throw new NullPointerException(
            // String.format("Rating Engine for Play %s is not defined.",
            // ratingEngine.getId()));
        } else {
            RatingsCountRequest request = new RatingsCountRequest();
            request.setRatingEngineIds(Arrays.asList(ratingEngine.getId()));
            RatingsCountResponse response = ratingCoverageService.getCoverageInfo(request);
            Map<String, CoverageInfo> ratingEngineIdCoverageMap = response.getSegmentIdCoverageMap();
            long accountCount = ratingEngineIdCoverageMap.get(ratingEngine.getId()).getAccountCount();
            long contactCount = ratingEngineIdCoverageMap.get(ratingEngine.getId()).getContactCount();
            List<RatingBucketCoverage> bucketCoverageCounts = ratingEngineIdCoverageMap.get(ratingEngine.getId())
                    .getBucketCoverageCounts();
            log.info(String.format("For play %s, new account number and contact number are %d and %d, respectively",
                    play.getName(), accountCount, contactCount));

            long mostRecentSucessfulLaunchAccountNum = playLaunch == null ? 0 : playLaunch.getAccountsLaunched();
            long mostRecentSucessfulLaunchContactNum = playLaunch == null ? 0 : playLaunch.getContactsLaunched();
            launchHistory.setNewAccountsNum(accountCount - mostRecentSucessfulLaunchAccountNum);
            launchHistory.setNewContactsNum(contactCount - mostRecentSucessfulLaunchContactNum);
        }
        // ----------------------------------------------------------------------------------------------

        // ----------------------------------------------------------------------------------------------
        // TODO in M15, we will get real data for AccountRatingMap
        // for now, just mock them
        RatingObject rating = new RatingObject();
        List<BucketInformation> accountRatingList = new ArrayList<>();
        BucketInformation aBucket = new BucketInformation();
        aBucket.setBucket(BucketName.A.name());
        aBucket.setBucketCount(500);
        BucketInformation bBucket = new BucketInformation();
        bBucket.setBucket(BucketName.B.name());
        bBucket.setBucketCount(500);
        BucketInformation cBucket = new BucketInformation();
        cBucket.setBucket(BucketName.C.name());
        cBucket.setBucketCount(500);
        BucketInformation dBucket = new BucketInformation();
        dBucket.setBucket(BucketName.D.name());
        dBucket.setBucketCount(500);
        accountRatingList.add(aBucket);
        accountRatingList.add(bBucket);
        accountRatingList.add(cBucket);
        accountRatingList.add(dBucket);
        rating.setBucketInfoList(accountRatingList);
        play.setRating(rating);
        // ----------------------------------------------------------------------------------------------
        return play;
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
