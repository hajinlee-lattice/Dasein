package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.RatingEngineDependencyType;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
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
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Component
public class TestPlayCreationHelper {

    private static final Logger log = LoggerFactory.getLogger(TestPlayCreationHelper.class);

    public static final String SEGMENT_NAME = NamingUtils.timestamp("Segment");

    @Resource(name = "deploymentTestBed")
    private GlobalAuthDeploymentTestBed deploymentTestBed;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private SegmentService segmentService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private RatingProxy ratingProxy;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private TalkingPointProxy talkingPointProxy;

    private String tenantIdentifier;

    private Tenant tenant;

    private Play play;

    private RatingEngine ratingEngine;

    private PlayLaunch playLaunch;

    private MetadataSegment segment;

    private String playName;

    public void setupTenantAndPopuldateData() {
        tenant = deploymentTestBed.bootstrapForProduct(LatticeProduct.CG);
        tenantIdentifier = tenant.getId();
        cdlTestDataService.populateData(tenantIdentifier);

        tenant = tenantEntityMgr.findByTenantId(tenantIdentifier);
        MultiTenantContext.setTenant(tenant);
    }

    public Tenant getTenant() {
        return tenant;
    }

    public Play getPlay() {
        return play;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    public RatingEngine getRatingEngine() {
        return ratingEngine;
    }
    
    public String getPlayName() {
        return playName;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        MultiTenantContext.setTenant(tenant);
    }

    public void setupTenantAndCreatePlay() {
        setupTenantAndPopuldateData();

        Restriction accountRestriction = createAccountRestriction();
        Restriction contactRestriction = createContactRestriction();
        RatingRule ratingRule = createRatingRule();

        segment = createSegment(SEGMENT_NAME, accountRestriction, contactRestriction);
        ratingEngine = createRatingEngine(segment, ratingRule);

        cdlTestDataService.mockRatingTableWithSingleEngine(tenant.getId(), ratingEngine.getId(), null);

        getCrud();
        createPlayLaunch();

        Assert.assertNotNull(play);
        Assert.assertNotNull(playLaunch);
    }

    public MetadataSegment createSegment(String segmentName, Restriction accountRestriction, Restriction contactRestriction) {
        segment = new MetadataSegment();
        segment.setAccountRestriction(accountRestriction);
        segment.setContactRestriction(contactRestriction);
        segment.setDisplayName(segmentName);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentService
                .findByName(CustomerSpace.parse(tenant.getId()).toString(), createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        return retrievedSegment;
    }

    public RatingEngine createRatingEngine(MetadataSegment retrievedSegment, RatingRule ratingRule) {
        RatingEngine ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setStatus(RatingEngineStatus.ACTIVE);

        RatingEngine createdRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(),
                ratingEngine1);
        Assert.assertNotNull(createdRatingEngine);
        cdlTestDataService.mockRatingTableWithSingleEngine(tenant.getId(), createdRatingEngine.getId(), null);
        ratingEngine1.setId(createdRatingEngine.getId());

        List<RatingModel> models = ratingEngineProxy.getRatingModels(tenant.getId(), ratingEngine1.getId());
        for (RatingModel model : models) {
            if (model instanceof RuleBasedModel) {
                ((RuleBasedModel) model).setRatingRule(ratingRule);
                ratingEngineProxy.updateRatingModel(tenant.getId(), ratingEngine1.getId(), model.getId(),
                        model);
            }
        }

        ratingEngine1 = ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngine1.getId());
        ratingEngine = ratingEngine1;
        return ratingEngine1;
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        play.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        RatingEngine ratingEngine1 = new RatingEngine();
        ratingEngine1.setId(ratingEngine.getId());
        play.setRatingEngine(ratingEngine1);
        return play;
    }

    public void createPlayLaunch() {
        playLaunch = playProxy.createPlayLaunch(tenant.getId(), playName, createDefaultPlayLaunch(), false);
        assertPlayLaunch(playLaunch, false);
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

    private PlayLaunch createDefaultPlayLaunch() {
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setBucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.values())));
        return playLaunch;
    }

    public void getCrud() {
        List<Play> playList = playProxy.getPlays(tenant.getId(), null, null);
        int existingPlays = playList == null ? 0 : playList.size();
        Play createdPlay1 = playProxy.createOrUpdatePlay(tenant.getId(), createDefaultPlay());
        playName = createdPlay1.getName();
        play = createdPlay1;
        assertPlay(createdPlay1);
        Map<RatingEngineDependencyType, List<String>> dependencies = ratingEngineProxy
                .getRatingEngineDependencies(tenant.getId(), ratingEngine.getId());
        Assert.assertNotNull(dependencies);
        Assert.assertEquals(dependencies.size(), 1);
        Assert.assertNotNull(dependencies.get(RatingEngineDependencyType.Play));
        Assert.assertEquals(dependencies.get(RatingEngineDependencyType.Play).size(), 1);
        Assert.assertEquals(dependencies.get(RatingEngineDependencyType.Play).get(0), play.getDisplayName());

        List<TalkingPointDTO> tps = getTestTalkingPoints(playName);
        List<TalkingPointDTO> createTPResponse = talkingPointProxy.createOrUpdate(tps,
                CustomerSpace.parse(tenant.getId()).toString());
        Assert.assertNotNull(createTPResponse);

        Play createdPlay2 = playProxy.createOrUpdatePlay(tenant.getId(), createDefaultPlay());
        Assert.assertNotNull(createdPlay2);

        dependencies = ratingEngineProxy.getRatingEngineDependencies(tenant.getId(), ratingEngine.getId());
        Assert.assertNotNull(dependencies);
        Assert.assertEquals(dependencies.size(), 1);
        Assert.assertNotNull(dependencies.get(RatingEngineDependencyType.Play));
        Assert.assertEquals(dependencies.get(RatingEngineDependencyType.Play).size(), 2);

        playList = playProxy.getPlays(tenant.getId(), null, null);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), existingPlays + 2);

        playList = playProxy.getPlays(tenant.getId(), null, ratingEngine.getId());
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 2);

        Play retrievedPlay = playProxy.getPlay(tenant.getId(), playName);
        Assert.assertEquals(retrievedPlay.getTalkingPoints().size(), 2);

        String jsonValue = JsonUtils.serialize(retrievedPlay);
        Assert.assertNotNull(jsonValue);
        this.play = retrievedPlay;
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
        Assert.assertEquals(play.getName(), playName);
        Assert.assertNotNull(play.getRatingEngine());
        Assert.assertEquals(play.getRatingEngine().getId(), ratingEngine.getId());
    }

    private Restriction createAccountRestriction() {
        Restriction b1 = //
                createBucketRestriction(6, ComparisonType.EQUAL, //
                        BusinessEntity.Account, "COMPOSITE_RISK_SCORE");
        Restriction b2 = //
                createBucketRestriction(1, ComparisonType.EQUAL, //
                        BusinessEntity.Account, "PREMIUM_MARKETING_PRESCREEN");
        Restriction b3 = //
                createBucketRestriction(2, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "CloudTechnologies_ContactCenterManagement");
        Restriction b4 = //
                createBucketRestriction(4, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "BusinessTechnologiesSsl");
        Restriction b5 = //
                createBucketRestriction(3, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "BusinessTechnologiesAnalytics");

        Restriction innerLogical1 = LogicalRestriction.builder()//
                .and(Arrays.asList(b1, b2, b3, b4, b5)).build();
        Restriction innerLogical2 = LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();

        return LogicalRestriction.builder() //
                .and(Arrays.asList(innerLogical1, innerLogical2)).build();
    }

    private Restriction createBucketRestriction(Object val, ComparisonType comparisonType, BusinessEntity entityType,
            String attrName) {
        Bucket bucket = null;

        if (comparisonType == ComparisonType.EQUAL) {
            bucket = Bucket.valueBkt(comparisonType, Arrays.asList(val));
        } else if (comparisonType == ComparisonType.LESS_THAN) {
            bucket = Bucket.rangeBkt(null, val);
        }
        return new BucketRestriction(new AttributeLookup(entityType, attrName), bucket);
    }

    private Restriction createContactRestriction() {
        return LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();
    }

    public RatingProxy initRatingProxy() {
        return ratingProxy;
    }

    public EntityProxy initEntityProxy() {
        return entityProxy;
    }

    public void cleanupArtifacts() {
        try {
            log.info("Cleaning up play launch: " + playLaunch.getId());
            deletePlayLaunch(play.getName(), playLaunch.getId());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up play: " + play.getName());
            deletePlay(play.getName());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up rating engine: " + play.getRatingEngine().getId());
            ratingEngineEntityMgr.deleteById(play.getRatingEngine().getId());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up segment: " + segment.getName());
            segmentService.deleteSegmentByName(tenantIdentifier, segment.getName());
        } catch (Exception ex) {
            ignoreException(ex);
        }
    }

    public void deletePlay(String playName) {
        playProxy.deletePlay(tenant.getId(), playName);
    }

    public void deletePlayLaunch(String playName, String playLaunchId) {
        playProxy.deletePlayLaunch(tenant.getId(), playName, playLaunchId);
    }

    private void ignoreException(Exception ex) {
        log.info("Could not cleanup artifact. Ignoring exception: ", ex);
    }

    private RatingRule createRatingRule() {
        RatingRule ratingRule = new RatingRule();
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = populateBucketToRuleMap();
        ratingRule.setBucketToRuleMap(bucketToRuleMap);
        ratingRule.setDefaultBucketName(RatingBucketName.C.name());
        return ratingRule;
    }

    private TreeMap<String, Map<String, Restriction>> populateBucketToRuleMap() {
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = new TreeMap<>();
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.A, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GTE_AND_LT, BusinessEntity.Account, "LDC_Name", "A", "G");
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.C, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GTE_AND_LTE, BusinessEntity.Account, "LDC_Name", "h", "n");
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.D, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GT_AND_LT, BusinessEntity.Account, "LDC_Name", "A", "O");
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.D, FrontEndQueryConstants.CONTACT_RESTRICTION, null,
                null, null, null, null);
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.F, FrontEndQueryConstants.ACCOUNT_RESTRICTION, null,
                null, null, null, null);
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.F, FrontEndQueryConstants.CONTACT_RESTRICTION, null,
                null, null, null, null);

        return bucketToRuleMap;
    }

    private void populateBucketInfo(TreeMap<String, Map<String, Restriction>> bucketToRuleMap,
            boolean createConcreteRestriction, RatingBucketName bucketName, String key, ComparisonType comparisonType,
            BusinessEntity entity, String attrName, Object min, Object max) {
        Map<String, Restriction> bucketInfo = bucketToRuleMap.computeIfAbsent(bucketName.name(), k -> new HashMap<>());
        Restriction info;
        if (createConcreteRestriction) {
            AttributeLookup lhs = new AttributeLookup(entity, attrName);
            Bucket rhs = Bucket.rangeBkt(min, max);
            rhs.setComparisonType(comparisonType);
            info = new BucketRestriction(lhs, rhs);
        } else {
            info = LogicalRestriction.builder() //
                    .and(new ArrayList<>()).build();
        }
        bucketInfo.put(key, info);
    }

}
