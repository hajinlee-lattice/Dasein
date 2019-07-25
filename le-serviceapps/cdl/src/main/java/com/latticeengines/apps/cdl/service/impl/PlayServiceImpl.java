package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.TalkingPointService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.LaunchHistory;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.ratings.coverage.CoverageInfo;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;

@Component("playService")
public class PlayServiceImpl implements PlayService {

    private static Logger log = LoggerFactory.getLogger(PlayServiceImpl.class);

    @Value("${cdl.play.service.threadpool.size:20}")
    private Integer fetcherNum;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private TalkingPointService talkingPointService;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private PlayTypeService playTypeService;

    private ForkJoinPool tpForParallelStream;

    @PostConstruct
    public void init() {
        tpForParallelStream = ThreadPoolUtils.getForkJoinThreadPool("play-details-fetcher", fetcherNum);
    }

    @Override
    public Play createOrUpdate(Play play, String tenantId) {
        return createOrUpdate(play, true, tenantId);
    }

    @Override
    public Play createOrUpdate(Play play, boolean shouldLoadCoverage, String tenantId) {
        log.info(String.format("%s play %sfor tenant %s", //
                play.getName() == null ? "Creating" : "Updating", //
                play.getName() == null //
                        ? "" : String.format("with name: %s, ", play.getName()),
                tenantId));
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        MultiTenantContext.setTenant(tenant);
        play.setTenant(tenant);
        Play retrievedPlay = null;
        boolean shouldCreateNew = false;

        if (StringUtils.isBlank(play.getName())) {
            shouldCreateNew = true;
        } else {
            retrievedPlay = playEntityMgr.getPlayByName(play.getName(), true);
            if (retrievedPlay == null) {
                retrievedPlay = playEntityMgr.createPlay(play);
            }
        }

        if (shouldCreateNew) {
            if (StringUtils.isBlank(play.getDisplayName())) {
                throw new LedpException(LedpCode.LEDP_40065);
            }

            play.setName(play.generateNameStr());
            retrievedPlay = playEntityMgr.createPlay(play);
        } else {
            retrievedPlay = playEntityMgr.updatePlay(play, retrievedPlay);
        }
        return getFullPlay(retrievedPlay, shouldLoadCoverage, tenant);
    }

    @Override
    public List<Play> getAllPlays() {
        List<Play> plays = playEntityMgr.findAll();
        updateLastRefreshedDate(plays);
        return plays;
    }

    @Override
    public List<String> getAllDeletedPlayIds(boolean forCleanupOnly) {
        return playEntityMgr.getAllDeletedPlayIds(forCleanupOnly);
    }

    @Override
    public Play getPlayByName(String name, Boolean considerDeleted) {
        Tenant tenant = MultiTenantContext.getTenant();

        if (StringUtils.isBlank(name)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        Play play = playEntityMgr.getPlayByName(name, considerDeleted);
        if (play == null) {
            log.warn(String.format("Error finding play by name %s in tenant %s", name,
                    tenant != null ? tenant.getName() : "null"));
        }
        if (play != null && play.getRatingEngine() != null) {
            updateLastRefreshedDate(play.getRatingEngine());
            setBucketMetadata(tenant, play);
        }
        return play;
    }

    private void setBucketMetadata(Tenant tenant, Play play) {
        if (play.getRatingEngine() == null) {
            log.error(String.format("Cannot set metadata since no Rating Engine was defined for Play %s.",
                    play.getName()));
            return;
        }

        if (play.getRatingEngine().getBucketMetadata() == null) {
            if (play.getRatingEngine().getType() == RatingEngineType.RULE_BASED) {

                Map<String, Long> counts = play.getRatingEngine().getCountsAsMap();
                if (counts != null) {
                    play.getRatingEngine()
                            .setBucketMetadata(counts.keySet().stream() //
                                    .map(c -> new BucketMetadata(BucketName.fromValue(c), counts.get(c).intValue()))
                                    .collect(Collectors.toList()));
                }
            } else {
                String reId = play.getRatingEngine().getId();
                try {
                    List<BucketMetadata> latestABCDBuckets = bucketedScoreProxy.getPublishedBucketMetadataByModelGuid(
                            tenant.getId(),
                            ((AIModel) play.getRatingEngine().getPublishedIteration()).getModelSummaryId());
                    play.getRatingEngine().setBucketMetadata(latestABCDBuckets);
                } catch (Exception ex) {
                    log.error("Ignoring exception while loading latest ABCD" + " bucket of rating engine " + reId
                            + " to set bucket metadata for play", ex);
                }
            }
        }
    }

    @Override
    public List<Play> getAllFullPlays(boolean shouldLoadCoverage, String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        List<Play> plays;
        if (ratingEngineId == null) {
            plays = getAllPlays();
        } else {
            RatingEngine ratingEngine = validateRatingEngineId(tenant, ratingEngineId);
            plays = playEntityMgr.findAllByRatingEnginePid(ratingEngine.getPid());
        }

        if (shouldLoadCoverage && CollectionUtils.isNotEmpty(plays)) {
            Map<String, List<RatingEngine>> ratingEnginesMap = new ConcurrentHashMap<>();

            Set<Pair<String, RatingEngineType>> ratingEngineIdSet = plays.stream() //
                    .filter(p -> p.getRatingEngine() != null) //
                    .filter(p -> StringUtils.isNotBlank(p.getRatingEngine().getId())) //
                    .map(p -> {
                        String reId = p.getRatingEngine().getId();
                        if (!ratingEnginesMap.containsKey(reId)) {
                            ratingEnginesMap.put(reId, new ArrayList<>());
                        }
                        ratingEnginesMap.get(reId).add(p.getRatingEngine());
                        return new ImmutablePair<>(p.getRatingEngine().getId(), p.getRatingEngine().getType());
                    }) //
                    .collect(Collectors.toSet());

            if (CollectionUtils.isNotEmpty(ratingEngineIdSet)) {
                log.info("Handling unique rating engine ids: " + JsonUtils.serialize(ratingEngineIdSet)
                        + "for corresponding plays");
                Set<Pair<String, RatingEngineType>> rulesTypeRatingEngineIds = ratingEngineIdSet.stream() //
                        .filter(r -> r.getRight() == RatingEngineType.RULE_BASED) //
                        .collect(Collectors.toSet());
                Set<Pair<String, RatingEngineType>> aiTypeRatingEngineIds = ratingEngineIdSet.stream() //
                        .filter(r -> r.getRight() != RatingEngineType.RULE_BASED) //
                        .collect(Collectors.toSet());

                if (CollectionUtils.isNotEmpty(rulesTypeRatingEngineIds)) {
                    log.info("Trying to build bucket metadata for rule based rating engine ids: "
                            + JsonUtils.serialize(rulesTypeRatingEngineIds));
                    rulesTypeRatingEngineIds //
                            .forEach(pair -> {
                                String reId = pair.getLeft();
                                List<RatingEngine> ratingEngines = ratingEnginesMap.get(reId);
                                ratingEngines.forEach(r -> {
                                    Map<String, Long> counts = r.getCountsAsMap();
                                    if (counts != null) {
                                        r.setBucketMetadata(
                                                counts.keySet().stream() //
                                                        .map(c -> new BucketMetadata(BucketName.fromValue(c),
                                                                counts.get(c).intValue()))
                                                        .collect(Collectors.toList()));
                                    }
                                });
                            });
                }
                if (CollectionUtils.isNotEmpty(aiTypeRatingEngineIds)) {
                    log.info("Trying to load latest ABDC buckets for AI rating engine ids: "
                            + JsonUtils.serialize(aiTypeRatingEngineIds));
                    tpForParallelStream.submit(//
                            () -> //
                            aiTypeRatingEngineIds //
                                    .stream() //
                                    .parallel() //
                                    .forEach(pair -> {
                                        String reId = pair.getLeft();
                                        List<RatingEngine> ratingEngines = ratingEnginesMap.get(reId);
                                        try {
                                            ratingEngines.forEach(r -> r.setBucketMetadata(
                                                    bucketedScoreProxy.getPublishedBucketMetadataByModelGuid(
                                                            tenant.getId(), ((AIModel) r.getPublishedIteration())
                                                                    .getModelSummaryId())));
                                        } catch (Exception ex) {
                                            log.info("Ignoring exception while loading latest ABCD"
                                                    + " bucket of rating engine " + reId
                                                    + " to set bucket metadata for play", ex);
                                        }
                                    })) //
                            .join();
                }
            }
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
    public Play getFullPlayByName(String name, Boolean considerDeleted) {
        Play play = getPlayByName(name, considerDeleted);
        if (play != null) {
            play = getFullPlay(play, true, null);
        }
        return play;
    }

    private Play getFullPlay(Play play, boolean shouldLoadCoverage, Tenant tenant) {
        Date lastRefreshedDate = findLastRefreshedDate();
        return getFullPlay(play, shouldLoadCoverage, tenant, lastRefreshedDate);
    }

    private Play getFullPlay(Play play, boolean shouldLoadCoverage, Tenant tenant, Date lastRefreshedDate) {
        if (play == null) {
            return null;
        }
        RatingEngine ratingEngine = play.getRatingEngine();
        if (tenant != null) {
            MultiTenantContext.setTenant(tenant);
        }

        LaunchHistory launchHistory = updatePlayLaunchHistory(play);
        if (ratingEngine != null) {
            if (shouldLoadCoverage) {
                populateCoverageInfo(play, launchHistory, ratingEngine);
            }
            setBucketMetadata(tenant, play);
            updateLastRefreshedDate(ratingEngine, lastRefreshedDate);
        }
        return play;
    }

    private LaunchHistory updatePlayLaunchHistory(Play play) {
        LaunchHistory launchHistory = getLaunchHistoryForPlay(play);
        play.setLaunchHistory(launchHistory);
        return launchHistory;
    }

    private LaunchHistory getLaunchHistoryForPlay(Play play) {
        PlayLaunch lastIncompleteLaunch = playLaunchService.findLatestByPlayId(play.getPid(),
                Collections.singletonList(LaunchState.UnLaunched));
        PlayLaunch lastCompletedLaunch = playLaunchService.findLatestByPlayId(play.getPid(),
                Arrays.asList(LaunchState.Launched, LaunchState.PartialSync, LaunchState.Synced));
        PlayLaunch mostRecentLaunch = playLaunchService.findLatestByPlayId(play.getPid(), null);
        LaunchHistory launchHistory = new LaunchHistory();
        launchHistory.setLastIncompleteLaunch(lastIncompleteLaunch);
        launchHistory.setLastCompletedLaunch(lastCompletedLaunch);
        launchHistory.setMostRecentLaunch(mostRecentLaunch);
        return launchHistory;
    }

    private CoverageInfo getCoverageInfo(Play play) {
        CoverageInfo coverageInfo = new CoverageInfo();
        MetadataSegment segment = play.getTargetSegment();
        RatingEngine ratingEngine = play.getRatingEngine();
        if (segment != null) {
            coverageInfo.setAccountCount(segment.getAccounts());
            coverageInfo.setContactCount(segment.getContacts());
        }
        if (ratingEngine != null) {
            if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
                coverageInfo.setBucketCoverageCounts(CoverageInfo.fromCounts(ratingEngine.getCountsAsMap()));
            } else {
                Tenant tenant = MultiTenantContext.getTenant();
                List<BucketMetadata> buckets = bucketedScoreProxy.getPublishedBucketMetadataByModelGuid(tenant.getId(),
                        ((AIModel) ratingEngine.getPublishedIteration()).getModelSummaryId());
                coverageInfo.setBucketCoverageCounts(CoverageInfo.fromBuckets(buckets));
            }
        }
        return coverageInfo;
    }

    private void populateCoverageInfo(Play play, LaunchHistory launchHistory, RatingEngine ratingEngine) {
        try {
            PlayLaunch mostRecentSuccessfulPlayLaunch = launchHistory.getLastIncompleteLaunch();

            CoverageInfo coverageInfo = getCoverageInfo(play);
            Long accountCount = coverageInfo.getAccountCount();
            if (accountCount == null) {
                throw new IllegalStateException(
                        "Cannot find a valid account count from rating engine " + ratingEngine.getId());
            }
            Long contactCount = coverageInfo.getContactCount();
            if (contactCount == null) {
                throw new IllegalStateException(
                        "Cannot find a valid contact count from rating engine " + ratingEngine.getId());
            }
            play.setRatings(coverageInfo.getBucketCoverageCounts());

            log.info(String.format("For play %s, account number and contact number are %d and %d, respectively",
                    play.getName(), accountCount, contactCount));
            Long mostRecentSuccessfulLaunchAccountNum = mostRecentSuccessfulPlayLaunch == null ? 0L
                    : mostRecentSuccessfulPlayLaunch.getAccountsLaunched();
            Long mostRecentSuccessfulLaunchContactNum = mostRecentSuccessfulPlayLaunch == null ? 0L
                    : mostRecentSuccessfulPlayLaunch.getContactsLaunched();

            Long newAccountsNum = accountCount - mostRecentSuccessfulLaunchAccountNum;
            Long newContactsNum = contactCount - mostRecentSuccessfulLaunchContactNum;

            launchHistory.setNewAccountsNum(newAccountsNum);
            launchHistory.setNewContactsNum(newContactsNum);
        } catch (Exception ex) {
            // any exception in getting count info should not block
            // rendering of play listing
            log.info("Ignoring exception in get cont info", ex);
        }
    }

    @Override
    public void deleteByName(String name, Boolean hardDelete) {
        if (StringUtils.isBlank(name)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        if (hardDelete != Boolean.TRUE) {
            // soft delete all related launches first
            Long playPid = playEntityMgr.getPlayByName(name, false).getPid();
            List<PlayLaunch> launches = playLaunchService.findByPlayId(playPid, null);
            if (CollectionUtils.isNotEmpty(launches)) {
                launches.forEach(l -> playLaunchService.deleteByLaunchId(l.getId(), false));
            }
            List<PlayLaunchChannel> channels = playLaunchChannelService.getPlayLaunchChannels(name, false);
            if (CollectionUtils.isNotEmpty(channels)) {
                channels.forEach(c -> playLaunchChannelService.deleteByChannelId(c.getId(), false));
            }
        }

        playEntityMgr.deleteByName(name, hardDelete);
    }

    @Override
    public void publishTalkingPoints(String playName, String customerSpace) {
        if (StringUtils.isBlank(playName)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        Play play = playEntityMgr.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_18144, new String[] { playName });
        }

        List<PlayLaunch> launches = playLaunchService.findByPlayId(play.getPid(),
                Arrays.asList(LaunchState.Launched, LaunchState.Failed));

        if (CollectionUtils.isEmpty(launches)) {
            log.warn("Play " + playName
                    + " has not been launched yet, cannot publish talking points on public apis yet.");
            return;
        }
        talkingPointService.publish(playName);
        play.setLastTalkingPointPublishTime(new Date());
        playEntityMgr.update(play);
    }

    private void updateLastRefreshedDate(RatingEngine ratingEngine) {
        Date lastRefreshedDate = findLastRefreshedDate();
        updateLastRefreshedDate(ratingEngine, lastRefreshedDate);
    }

    private void updateLastRefreshedDate(RatingEngine ratingEngine, Date lastRefreshedDate) {
        ratingEngine.setLastRefreshedDate(lastRefreshedDate);
    }

    private void updateLastRefreshedDate(List<Play> plays) {
        if (CollectionUtils.isNotEmpty(plays)) {
            Date lastRefreshedDate = findLastRefreshedDate();
            plays //
                    .forEach(play -> {
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

    @Override
    public List<AttributeLookup> findDependingAttributes(List<Play> plays) {
        Tenant tenant = MultiTenantContext.getTenant();
        Set<AttributeLookup> dependingAttributes = new HashSet<>();
        if (plays != null) {
            for (Play play : plays) {
                dependingAttributes.addAll(talkingPointService.getAttributesInTalkingPointOfPlay(play.getName()));
            }
        }

        return new ArrayList<>(dependingAttributes);
    }

    @Override
    public List<Play> findDependingPalys(List<String> attributes) {
        Tenant tenant = MultiTenantContext.getTenant();
        Set<Play> dependingPlays = new HashSet<>();
        if (attributes != null) {
            log.info("getting play " + tenant.getId());
            List<Play> plays = getAllPlays();
            if (plays != null) {
                for (Play play : plays) {
                    List<AttributeLookup> playAttributes = talkingPointService
                            .getAttributesInTalkingPointOfPlay(play.getName());
                    for (AttributeLookup attributeLookup : playAttributes) {
                        if (attributes.contains(sanitize(attributeLookup.toString()))) {
                            dependingPlays.add(play);
                            break;
                        }
                    }
                }
            }
        }

        return new ArrayList<>(dependingPlays);
    }

    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }
}
