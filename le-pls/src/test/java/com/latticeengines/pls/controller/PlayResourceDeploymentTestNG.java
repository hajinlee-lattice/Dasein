package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

@Component
public class PlayResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PlayResourceDeploymentTestNG.class);

    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private Play play;
    private String name;
    private PlayLaunch playLaunch;

    private Tenant tenant;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private SegmentProxy segmentProxy;

    private RatingEngine ratingEngine1;
    private MetadataSegment segment;
    @Inject
    private PlayProxy playProxy;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private boolean shouldSkipAutoTenantCreation = false;

    private boolean shouldSkipCdlTestDataPopulation = false;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        if (shouldSkipAutoTenantCreation) {
            tenant = mainTestTenant;
        } else {
            setupTestEnvironmentWithOneTenant();
            tenant = testBed.getMainTestTenant();
        }
        switchToSuperAdmin();

        if (!shouldSkipCdlTestDataPopulation) {
            cdlTestDataService.populateData(tenant.getId());
        }

        MetadataSegment retrievedSegment = createSegment();

        createRatingEngine(retrievedSegment, new RatingRule());
    }

    public RatingEngine createRatingEngine(MetadataSegment retrievedSegment, RatingRule ratingRule) {
        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setStatus(RatingEngineStatus.ACTIVE);

        RatingEngine createdRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(), ratingEngine1);
        Assert.assertNotNull(createdRatingEngine);
        cdlTestDataService.mockRatingTableWithSingleEngine(tenant.getId(), createdRatingEngine.getId(), null);

        ratingEngine1.setId(createdRatingEngine.getId());

        List<RatingModel> models = ratingEngineProxy.getRatingModels(tenant.getId(), ratingEngine1.getId());
        for (RatingModel model : models) {
            if (model instanceof RuleBasedModel) {
                ((RuleBasedModel) model).setRatingRule(ratingRule);
                ratingEngineProxy.updateRatingModel(tenant.getId(), ratingEngine1.getId(), model.getId(), model);
            }
        }

        ratingEngine1 = ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngine1.getId());
        return ratingEngine1;
    }

    MetadataSegment createSegment() {
        return createSegment(null, null);
    }

    public MetadataSegment createSegment(Restriction accountRestriction, Restriction contactRestriction) {
        segment = new MetadataSegment();
        segment.setAccountRestriction(accountRestriction);
        segment.setContactRestriction(contactRestriction);
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentProxy
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentProxy
                .getMetadataSegmentByName(CustomerSpace.parse(tenant.getId()).toString(), createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        return retrievedSegment;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment")
    public void getCrud() {
        logInterceptor();

        int existingPlays = createPlayOnly();

        List<TalkingPointDTO> tps = getTestTalkingPoints(name);
        List<TalkingPointDTO> createTPResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints", //
                tps, //
                List.class);
        Assert.assertNotNull(createTPResponse);

        Play createdPlay2 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", createDefaultPlay(),
                Play.class);
        Assert.assertNotNull(createdPlay2);

        List<Play> playList = (List) restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        Assert.assertNotNull(playList);

        Assert.assertEquals(playList.size(), existingPlays + 2);

        playList = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play?ratingEngineId=" + ratingEngine1.getId(),
                List.class);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 2);

        Play retrievedPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        assertPlay(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getTalkingPoints().size(), 2);

        String jsonValue = JsonUtils.serialize(retrievedPlay);
        Assert.assertNotNull(jsonValue);
        this.play = retrievedPlay;
    }

    public int createPlayOnly() {
        List<?> playList = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        int existingPlaysCount = playList == null ? 0 : playList.size();
        Play createdPlay1 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", createDefaultPlay(),
                Play.class);
        name = createdPlay1.getName();
        play = createdPlay1;
        assertPlay(createdPlay1);
        return existingPlaysCount;
    }

    @Test(groups = "deployment", dependsOnMethods = { "getCrud" })
    public void createPlayLaunch() {
        createPlayLaunch(false);
    }

    public void createPlayLaunch(boolean isDryRunMode) {
        logInterceptor();

        playLaunch = restTemplate.postForObject(
                getRestAPIHostPort() + //
                        "/pls/play/" + name + "/launches?dry-run=" + isDryRunMode,
                createDefaultPlayLaunch(), PlayLaunch.class);

        assertPlayLaunch(playLaunch, isDryRunMode);
    }

    @Test(groups = "deployment", dependsOnMethods = { "createPlayLaunch" })
    public void createPlayLaunchFail1() {
        // TODO - enable it once UI has fix for PLS-6769
        //
        // PlayLaunch launch = createDefaultPlayLaunch();
        // launch.setBucketsToLaunch(new HashSet<>());
        // try {
        // launch = restTemplate.postForObject(getRestAPIHostPort() + //
        // "/pls/play/" + name + "/launches", launch, PlayLaunch.class);
        // Assert.fail("Play launch submission should fail");
        // } catch (Exception ex) {
        // Assert.assertTrue(ex.getMessage().contains(LedpCode.LEDP_18156.name()));
        // }
    }

    @Test(groups = "deployment", dependsOnMethods = { "createPlayLaunchFail1" })
    public void createPlayLaunchFail2() {
        PlayLaunch launch = createDefaultPlayLaunch();
        launch.setBucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.F)));
        try {
            launch = restTemplate.postForObject(getRestAPIHostPort() + //
                    "/pls/play/" + name + "/launches", launch, PlayLaunch.class);
            Assert.fail("Play launch submission should fail");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains(LedpCode.LEDP_18176.name()));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment", dependsOnMethods = { "createPlayLaunchFail2" })
    private void searchPlayLaunch() {
        List<PlayLaunch> launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Failed, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

        playProxy.updatePlayLaunch(tenant.getId(), name, playLaunch.getLaunchId(), LaunchState.Launched);
        playProxy.updatePlayLaunchProgress(tenant.getId(), name, playLaunch.getLaunchId(), 100.0D, 10L, 8L, 25L, 0L,
                2L);

        launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Canceled + "&launchStates="
                + LaunchState.Failed + "&launchStates=" + LaunchState.Launched, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Launched, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches", List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Launching, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

        PlayLaunch retrievedLaunch = restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches/" + playLaunch.getLaunchId(), PlayLaunch.class);
        Assert.assertNotNull(retrievedLaunch);
        Assert.assertEquals(retrievedLaunch.getLaunchState(), LaunchState.Launched);
        assertLaunchStats(retrievedLaunch.getAccountsSelected(), 10L);
        assertLaunchStats(retrievedLaunch.getAccountsLaunched(), 8L);
        assertLaunchStats(retrievedLaunch.getContactsLaunched(), 25L);
        assertLaunchStats(retrievedLaunch.getAccountsErrored(), 0L);
        assertLaunchStats(retrievedLaunch.getAccountsSuppressed(), 2L);
    }

    private void assertLaunchStats(Long count, long expectedVal) {
        Assert.assertNotNull(count);
        Assert.assertEquals(count.longValue(), expectedVal);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "searchPlayLaunch" })
    private void testGetFullPlays() {
        Play retrievedFullPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        Assert.assertNotNull(retrievedFullPlay);
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getPlayLaunch());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getMostRecentLaunch());
        // TODO will change to NotNull after integration with RatingEngine is
        // fully done
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewAccountsNum());
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewContactsNum());
        System.out.println("retrievedPlayOverview is " + retrievedFullPlay);

        List<Play> retrievedFullPlayList = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play", List.class);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "testGetFullPlays" })
    private void testIdempotentCreateOrUpdatePlays() {
        Play createdPlay1 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", play, Play.class);
        assertPlay(createdPlay1);
        Assert.assertNotNull(createdPlay1.getTalkingPoints());

        List<Play> retrievedFullPlayList = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play", List.class);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment", dependsOnMethods = { "testIdempotentCreateOrUpdatePlays" })
    public void testDeletePlayLaunch() {
        deletePlayLaunch(name, playLaunch.getLaunchId());

        List<PlayLaunch> launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Launched, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment", dependsOnMethods = { "testDeletePlayLaunch" })
    private void testPlayDelete() {
        List<Play> playList;
        Play retrievedPlay;
        deletePlay(name);
        retrievedPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        Assert.assertNull(retrievedPlay);
        playList = (List) restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
    }

    public void deletePlay(String playName) {
        restTemplate.delete(getRestAPIHostPort() + "/pls/play/" + playName);
    }

    public void deletePlayLaunch(String playName, String playLaunchId) {
        restTemplate.delete(getRestAPIHostPort() + "/pls/play/" + playName + "/launches/" + playLaunchId);
    }

    private void assertPlayLaunch(PlayLaunch playLaunch, boolean isDryRunMode) {
        Assert.assertNotNull(playLaunch);
        Assert.assertNotNull(playLaunch.getLaunchId());
        Assert.assertNotNull(playLaunch.getPid());
        Assert.assertNotNull(playLaunch.getUpdated());
        Assert.assertNotNull(playLaunch.getCreated());
        if (isDryRunMode) {
            Assert.assertNull(playLaunch.getApplicationId());
        } else {
            Assert.assertNotNull(playLaunch.getApplicationId());
        }
        Assert.assertNotNull(playLaunch.getLaunchState());
        assertBucketsToLaunch(playLaunch.getBucketsToLaunch());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.Launching);
    }

    private void assertBucketsToLaunch(Set<RatingBucketName> bucketsToLaunch) {
        Assert.assertNotNull(playLaunch.getBucketsToLaunch());
        Set<RatingBucketName> defaultBucketsToLaunch = new TreeSet<>(Arrays.asList(RatingBucketName.values()));
        Assert.assertEquals(bucketsToLaunch.size(), defaultBucketsToLaunch.size());
        for (RatingBucketName bucket : bucketsToLaunch) {
            Assert.assertTrue(defaultBucketsToLaunch.contains(bucket));
        }
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        play.setCreatedBy(CREATED_BY);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngine1.getId());
        play.setRatingEngine(ratingEngine);
        return play;
    }

    private List<TalkingPointDTO> getTestTalkingPoints(String playName) {
        List<TalkingPointDTO> tps = new ArrayList<>();
        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setName("plsTP1" + UUID.randomUUID());
        tp.setPlayName(playName);
        tp.setOffset(1);
        tp.setTitle("Test TP Title");
        tp.setContent("PLS Deployment Test Talking Point no 1");
        tps.add(tp);

        TalkingPointDTO tp1 = new TalkingPointDTO();

        tp1.setName("plsTP2" + UUID.randomUUID());
        tp1.setPlayName(playName);
        tp1.setOffset(2);
        tp1.setTitle("Test TP2 Title");
        tp1.setContent("PLS Deployment Test Talking Point no 2");
        tps.add(tp1);

        return tps;
    }

    private void assertPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getName(), name);
        Assert.assertNotNull(play.getRatingEngine());
        Assert.assertEquals(play.getRatingEngine().getId(), ratingEngine1.getId());
    }

    public Play getPlay() {
        return play;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    private PlayLaunch createDefaultPlayLaunch() {
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setBucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.values())));
        playLaunch.setDestinationOrgId("o_" + System.currentTimeMillis());
        playLaunch.setDestinationSysType(CDLExternalSystemType.CRM);
        playLaunch.setDestinationAccountId(InterfaceName.SalesforceAccountID.name());
        return playLaunch;
    }

    public void setShouldSkipAutoTenantCreation(boolean shouldSkipAutoTenantCreation) {
        this.shouldSkipAutoTenantCreation = shouldSkipAutoTenantCreation;
    }

    public void setShouldSkipCdlTestDataPopulation(boolean shouldSkipCdlTestDataPopulation) {
        this.shouldSkipCdlTestDataPopulation = shouldSkipCdlTestDataPopulation;
    }

    public RatingEngine getRatingEngine() {
        return this.ratingEngine1;
    }

    private void logInterceptor() {
        log.info("Tenant = " + tenant.getId());
        log.info("restTemplate = " + restTemplate);
        restTemplate.getInterceptors().stream() //
                .forEach(
                        in -> log.info(String.format("interceptor Obj = %s, class = %s", in, in.getClass().getName())));
    }
}
