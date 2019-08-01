package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.ratings.coverage.CoverageInfo;
import com.latticeengines.domain.exposed.ratings.coverage.ProductsCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingBucketCoverage;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.ratings.coverage.RatingIdLookupColumnPair;
import com.latticeengines.domain.exposed.ratings.coverage.RatingModelIdPair;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;
import com.latticeengines.domain.exposed.ratings.coverage.SegmentIdAndModelRulesPair;
import com.latticeengines.domain.exposed.ratings.coverage.SegmentIdAndSingleRulePair;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingCoverageProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.testframework.exposed.domain.TestPlayChannelConfig;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public class RatingCoverageServiceImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(RatingCoverageServiceImplDeploymentTestNG.class);

    private static final String DUMMY_ID = "DUMMY_ID";

    @Inject
    private RatingCoverageProxy ratingCoverageProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    private Play play;

    private RatingEngine ratingEngine;

    private RatingRule ratingRule;

    @Autowired
    private RatingCoverageService ratingCoverageService;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        String existingTenant = null;

        final TestPlaySetupConfig testPlaySetupConfig = new TestPlaySetupConfig.Builder()
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.CRM)
                        .destinationSystemName(CDLExternalSystemName.Salesforce)
                        .destinationSystemId("test_sfdc_" + System.currentTimeMillis()).build())
                .existingTenant(existingTenant).mockRatingTable(true).build();
        testPlayCreationHelper.setupTenantAndCreatePlay(testPlaySetupConfig);

        play = testPlayCreationHelper.getPlay();
        String ratingEngineId = play.getRatingEngine().getId();
        ratingEngine = ratingEngineProxy.getRatingEngine(testPlayCreationHelper.getTenant().getId(), ratingEngineId);

        Assert.assertNotNull(ratingEngine);
        List<RatingModel> ratingModels = ratingEngineProxy.getRatingModels(testPlayCreationHelper.getTenant().getId(),
                play.getRatingEngine().getId());
        Assert.assertNotNull(ratingModels);
        Assert.assertTrue(ratingModels.size() > 0);
        Assert.assertTrue(ratingModels.get(0) instanceof RuleBasedModel);
        RuleBasedModel ruleBasedModel = (RuleBasedModel) ratingEngine.getLatestIteration();
        Assert.assertNotNull(ruleBasedModel);

        ratingRule = ruleBasedModel.getRatingRule();

        Assert.assertNotNull(ratingRule);
        Assert.assertNotNull(ratingRule.getBucketToRuleMap());
        Assert.assertNotNull(ratingRule.getDefaultBucketName());
    }

    @AfterClass(groups = { "deployment-app" })
    public void teardown() throws Exception {
        testPlayCreationHelper.cleanupArtifacts(true);
        log.info("Cleaned up all artifacts");
    }

    @Test(groups = "deployment-app")
    public void testRatingIdCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();

        List<String> ratingEngineIds = Arrays.asList(ratingEngine.getId(), DUMMY_ID);
        request.setRatingEngineIds(ratingEngineIds);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getRatingEngineIdCoverageMap().size(), 1);

        for (String ratingId : ratingEngineIds) {
            if (ratingId.equals(DUMMY_ID)) {
                Assert.assertNotNull(response.getErrorMap());
                Assert.assertTrue(response.getErrorMap().get(RatingCoverageService.RATING_IDS_ERROR_MAP_KEY)
                        .containsKey(ratingId));
                Assert.assertNotNull(
                        response.getErrorMap().get(RatingCoverageService.RATING_IDS_ERROR_MAP_KEY).get(ratingId));
                Assert.assertFalse(response.getRatingEngineIdCoverageMap().containsKey(ratingId));
            } else {
                Assert.assertTrue(response.getRatingEngineIdCoverageMap().containsKey(ratingId));
                Assert.assertNotNull(response.getRatingEngineIdCoverageMap().get(ratingId));
                Assert.assertFalse(response.getErrorMap().get(RatingCoverageService.RATING_IDS_ERROR_MAP_KEY)
                        .containsKey(ratingId));

                Set<String> uniqueBuckets = new HashSet<>();
                uniqueBuckets.addAll(ratingRule.getBucketToRuleMap().keySet());
                uniqueBuckets.add(ratingRule.getDefaultBucketName());

                log.info("Unique Buckets: " + uniqueBuckets);
                log.info("Ratings Coverage: "
                        + JsonUtils.serialize(response.getRatingEngineIdCoverageMap().get(ratingId)));
            }
        }
    }

    @Test(groups = "deployment-app")
    public void testSegmentIdCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();
        List<String> segmentIds = Arrays.asList(play.getRatingEngine().getSegment().getName(), DUMMY_ID);
        request.setSegmentIds(segmentIds);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getSegmentIdCoverageMap().size(), 1);

        for (String segmentId : segmentIds) {
            if (segmentId.equals(DUMMY_ID)) {
                Assert.assertNotNull(response.getErrorMap());
                Assert.assertTrue(response.getErrorMap().get(RatingCoverageService.SEGMENT_IDS_ERROR_MAP_KEY)
                        .containsKey(segmentId));
                Assert.assertNotNull(
                        response.getErrorMap().get(RatingCoverageService.SEGMENT_IDS_ERROR_MAP_KEY).get(segmentId));
                Assert.assertFalse(response.getSegmentIdCoverageMap().containsKey(segmentId));
            } else {
                Assert.assertTrue(response.getSegmentIdCoverageMap().containsKey(segmentId));
                Assert.assertNotNull(response.getSegmentIdCoverageMap().get(segmentId));
                Assert.assertFalse(response.getErrorMap().get(RatingCoverageService.SEGMENT_IDS_ERROR_MAP_KEY)
                        .containsKey(segmentId));
            }
        }
    }

    @Test(groups = "deployment-app")
    public void testRatingModelIdCoverage() {
        @SuppressWarnings("deprecation")
        RuleBasedModel ruleBasedModel = (RuleBasedModel) ratingEngine.getLatestIteration();
        RatingsCountRequest request = new RatingsCountRequest();
        RatingModelIdPair p1 = new RatingModelIdPair();
        p1.setRatingEngineId(ratingEngine.getId());
        p1.setRatingModelId(ruleBasedModel.getId());
        RatingModelIdPair p2 = new RatingModelIdPair();
        p2.setRatingEngineId(DUMMY_ID);
        p2.setRatingModelId(DUMMY_ID);
        List<RatingModelIdPair> ratingEngineModelIds = Arrays.asList(p1, p2);
        request.setRatingEngineModelIds(ratingEngineModelIds);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNotNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getRatingEngineModelIdCoverageMap().size(), ratingEngineModelIds.size());

        for (RatingModelIdPair ratingModelId : ratingEngineModelIds) {
            Assert.assertTrue(
                    response.getRatingEngineModelIdCoverageMap().containsKey(ratingModelId.getRatingModelId()));
            Assert.assertNotNull(response.getRatingEngineModelIdCoverageMap().get(ratingModelId.getRatingModelId()));
        }
    }

    @Test(groups = "deployment-app")
    public void testRatingIdLookupColumnPairCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();
        String p1Key = "p1Key";
        String p2Key = "p2Key";
        String p3Key = "p3Key";
        String p4Key = "p4Key";
        String p5Key = null;

        RatingIdLookupColumnPair p1 = new RatingIdLookupColumnPair();
        p1.setRatingEngineId(ratingEngine.getId());
        p1.setResponseKeyId(p1Key);
        p1.setLookupColumn(InterfaceName.SalesforceAccountID.name());
        RatingIdLookupColumnPair p2 = new RatingIdLookupColumnPair();
        p2.setRatingEngineId(ratingEngine.getId());
        p2.setResponseKeyId(p2Key);
        p2.setLookupColumn(InterfaceName.CompanyName.name());
        RatingIdLookupColumnPair p3 = new RatingIdLookupColumnPair();
        p3.setRatingEngineId(ratingEngine.getId());
        p3.setResponseKeyId(p3Key);
        p3.setLookupColumn("DUMMY_COLUMN");
        RatingIdLookupColumnPair p4 = new RatingIdLookupColumnPair();
        p4.setRatingEngineId("DUMMY_ID");
        p4.setResponseKeyId(p4Key);
        p4.setLookupColumn(InterfaceName.SalesforceAccountID.name());
        RatingIdLookupColumnPair p5 = new RatingIdLookupColumnPair();
        p5.setRatingEngineId(ratingEngine.getId());
        p5.setResponseKeyId(p5Key);
        p5.setLookupColumn(InterfaceName.SalesforceAccountID.name());
        request.setRestrictNotNullSalesforceId(true);

        List<RatingIdLookupColumnPair> ratingIdLookupColumnPairs = Arrays.asList(p1, p2, p3, p4, p5);
        Set<RatingIdLookupColumnPair> badOnes = new HashSet<>(
                Arrays.asList(new RatingIdLookupColumnPair[] { p3, p4, p5 }));
        request.setRatingIdLookupColumnPairs(ratingIdLookupColumnPairs);

        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNotNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getRatingIdLookupColumnPairsCoverageMap().size(),
                ratingIdLookupColumnPairs.size() - badOnes.size(), //
                String.format("Request = %s\nResponse = %s", //
                        JsonUtils.serialize(request), JsonUtils.serialize(response)));

        for (RatingIdLookupColumnPair ratingIdLookupColumnPair : ratingIdLookupColumnPairs) {
            String key = ratingIdLookupColumnPair.getResponseKeyId();

            if (badOnes.contains(ratingIdLookupColumnPair)) {
                Assert.assertFalse(response.getRatingIdLookupColumnPairsCoverageMap()
                        .containsKey(ratingIdLookupColumnPair.getResponseKeyId()));
                key = key == null ? "NULL_KEY" : key;

                Assert.assertNotNull(response.getErrorMap());
                Map<String, String> errorMap = response.getErrorMap().get("processRatingIdLookupColumnPairs");
                Assert.assertNotNull(errorMap);
                Assert.assertTrue(errorMap.containsKey(key));
                Assert.assertNotNull(errorMap.get(key));
            } else {
                Assert.assertTrue(response.getRatingIdLookupColumnPairsCoverageMap().containsKey(key));
                CoverageInfo coverage = response.getRatingIdLookupColumnPairsCoverageMap().get(key);
                Assert.assertNotNull(coverage);
                Assert.assertNotNull(coverage.getAccountCount());
                Assert.assertNotNull(coverage.getContactCount());
                Assert.assertNotNull(coverage.getBucketCoverageCounts());
            }
        }
    }

    @Test(groups = "deployment-app")
    public void testSegmentIdModelRulesCoverage() {
        @SuppressWarnings("deprecation")
        RuleBasedModel ruleBasedModel = (RuleBasedModel) ratingEngine.getLatestIteration();
        RatingsCountRequest request = new RatingsCountRequest();
        SegmentIdAndModelRulesPair r1 = new SegmentIdAndModelRulesPair();
        r1.setSegmentId(ratingEngine.getSegment().getName());
        r1.setRatingRule(ruleBasedModel.getRatingRule());
        List<SegmentIdAndModelRulesPair> segmentIdModelRules = Collections.singletonList(r1);
        request.setSegmentIdModelRules(segmentIdModelRules);
        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);
        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdModelRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getSegmentIdModelRulesCoverageMap().size(), segmentIdModelRules.size());

        for (SegmentIdAndModelRulesPair segmentIdModelPair : segmentIdModelRules) {
            Assert.assertTrue(
                    response.getSegmentIdModelRulesCoverageMap().containsKey(segmentIdModelPair.getSegmentId()));
            Assert.assertNotNull(response.getSegmentIdModelRulesCoverageMap().get(segmentIdModelPair.getSegmentId()));
        }
    }

    @Test(groups = "deployment-app")
    public void testSegmentIdSingleRulesCoverage() {
        RatingsCountRequest request = new RatingsCountRequest();

        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Account, "LDC_Name");
        Bucket bucket1 = Bucket.rangeBkt("A", "G", true, false);
        Bucket bucket2 = Bucket.rangeBkt("B", "Z", true, false);
        Restriction accountRestriction1 = new BucketRestriction(attrLookup, bucket1);
        Restriction accountRestriction2 = new BucketRestriction(attrLookup, bucket2);

        SegmentIdAndSingleRulePair r1 = new SegmentIdAndSingleRulePair();
        r1.setSegmentId(ratingEngine.getSegment().getName());
        r1.setResponseKeyId("R1");
        r1.setAccountRestriction(accountRestriction1);

        SegmentIdAndSingleRulePair r2 = new SegmentIdAndSingleRulePair();
        r2.setSegmentId(ratingEngine.getSegment().getName());
        r2.setResponseKeyId("R2");

        r2.setAccountRestriction(accountRestriction2);

        List<SegmentIdAndSingleRulePair> segmentIdAndSingleRulePairs = Arrays.asList(r1, r2);
        request.setSegmentIdAndSingleRules(segmentIdAndSingleRulePairs);

        RatingsCountResponse response = ratingCoverageProxy.getCoverageInfo(testPlayCreationHelper.getTenant().getId(),
                request);

        Assert.assertNotNull(response);
        Assert.assertNull(response.getRatingEngineIdCoverageMap());
        Assert.assertNull(response.getRatingEngineModelIdCoverageMap());
        Assert.assertNull(response.getSegmentIdCoverageMap());
        Assert.assertNotNull(response.getSegmentIdAndSingleRulesCoverageMap());
        Assert.assertNull(response.getRatingIdLookupColumnPairsCoverageMap());

        Assert.assertEquals(response.getSegmentIdAndSingleRulesCoverageMap().size(),
                segmentIdAndSingleRulePairs.size());

        for (SegmentIdAndSingleRulePair segmentIdAndSingleRulePair : segmentIdAndSingleRulePairs) {
            Assert.assertTrue(response.getSegmentIdAndSingleRulesCoverageMap()
                    .containsKey(segmentIdAndSingleRulePair.getResponseKeyId()));
            Assert.assertNotNull(response.getSegmentIdAndSingleRulesCoverageMap()
                    .get(segmentIdAndSingleRulePair.getResponseKeyId()));
        }
    }

    @Test(groups = "deployment-app")
    public void testRatingModelsCoverageForSegment() {
        RatingEnginesCoverageRequest request = new RatingEnginesCoverageRequest();
        String ratingId1 = ratingEngine.getId();
        List<String> ratingEngineIds = Collections.singletonList(ratingId1);
        request.setRatingEngineIds(ratingEngineIds);

        // Check with Segment Criteria ACT_ATTR_PREMIUM_MARKETING_PRESCREEN = 1
        MetadataSegment currentSegment = ratingEngine.getSegment();
        RatingEnginesCoverageResponse response = ratingCoverageService.getRatingCoveragesForSegment(
                testPlayCreationHelper.getTenant().getId(), currentSegment.getName(), request);
        assertRatingModelsCoverageResponse(ratingId1, response, request);
        log.info("Rating Engine Segment {}, Account Count:{}, Contact Count: {}", currentSegment.getName(),
                currentSegment.getAccounts(), currentSegment.getContacts());
        log.info("Ratings Coverage: " + JsonUtils.serialize(response.getRatingModelsCoverageMap().get(ratingId1)));
        CoverageInfo coverage1 = response.getRatingModelsCoverageMap().get(ratingId1);

        // Check with Segment Criteria ACT_ATTR_PREMIUM_MARKETING_PRESCREEN = 2
        currentSegment = testPlayCreationHelper.createTargetSegment();
        response = ratingCoverageService.getRatingCoveragesForSegment(testPlayCreationHelper.getTenant().getId(),
                currentSegment.getName(), request);
        assertRatingModelsCoverageResponse(ratingId1, response, request);
        log.info("Rating Engine Segment {}, Account Count:{}, Contact Count: {}", currentSegment.getName(),
                currentSegment.getAccounts(), currentSegment.getContacts());
        log.info("Ratings Coverage: " + JsonUtils.serialize(response.getRatingModelsCoverageMap().get(ratingId1)));
        CoverageInfo coverage2 = response.getRatingModelsCoverageMap().get(ratingId1);

        // Check with Segment Criteria ACT_ATTR_PREMIUM_MARKETING_PRESCREEN < 3
        // And check whether the coverage counts matches with other 2 segment
        // coverages
        currentSegment = testPlayCreationHelper.createAggregatedSegment();
        response = ratingCoverageService.getRatingCoveragesForSegment(testPlayCreationHelper.getTenant().getId(),
                currentSegment.getName(), request);
        assertRatingModelsCoverageResponse(ratingId1, response, request);
        log.info("Rating Engine Segment {}, Account Count:{}, Contact Count: {}", currentSegment.getName(),
                currentSegment.getAccounts(), currentSegment.getContacts());
        log.info("Ratings Coverage: " + JsonUtils.serialize(response.getRatingModelsCoverageMap().get(ratingId1)));
        CoverageInfo coverageAggregated = response.getRatingModelsCoverageMap().get(ratingId1);

        assertEquals(new Long(coverage1.getAccountCount() + coverage2.getAccountCount()),
                coverageAggregated.getAccountCount());
        coverageAggregated.getBucketCoverageCounts().forEach(rbc -> {
            RatingBucketCoverage coverage1Bucket = coverage1.getCoverageForBucket(rbc.getBucket());
            RatingBucketCoverage coverage2Bucket = coverage2.getCoverageForBucket(rbc.getBucket());
            Long computedCnt = (coverage1Bucket != null && coverage1Bucket.getCount() != null
                    ? coverage1Bucket.getCount()
                    : 0)
                    + (coverage2Bucket != null && coverage2Bucket.getCount() != null ? coverage2Bucket.getCount() : 0);
            Assert.assertEquals(computedCnt, rbc.getCount(), "Bucket Count doesnot match for: " + rbc.getBucket());
        });
    }

    private void assertRatingModelsCoverageResponse(String ratingId1, RatingEnginesCoverageResponse response,
            RatingEnginesCoverageRequest request) {
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getRatingModelsCoverageMap());
        Assert.assertNotNull(response.getErrorMap());
        Assert.assertTrue(response.getErrorMap().isEmpty());

        Assert.assertEquals(response.getRatingModelsCoverageMap().size(), 1);
        Assert.assertTrue(response.getRatingModelsCoverageMap().containsKey(ratingId1));
        Assert.assertNotNull(response.getRatingModelsCoverageMap().get(ratingId1));

        if (request.isLoadContactsCount()) {
            Assert.assertNotNull(response.getRatingModelsCoverageMap().get(ratingId1).getContactCount());
        } else {
            Assert.assertNull(response.getRatingModelsCoverageMap().get(ratingId1).getContactCount());
        }
    }

    @Test(groups = "deployment-app")
    public void testRatingModelsCoverageWithContactsForSegment() {

        RatingEnginesCoverageRequest request = new RatingEnginesCoverageRequest();
        String ratingId1 = ratingEngine.getId();
        List<String> ratingEngineIds = Collections.singletonList(ratingId1);
        request.setRatingEngineIds(ratingEngineIds);
        request.setLoadContactsCount(true);
        request.setLoadContactsCountByBucket(true);

        // Check with Segment Criteria ACT_ATTR_PREMIUM_MARKETING_PRESCREEN = 1
        MetadataSegment currentSegment = ratingEngine.getSegment();
        RatingEnginesCoverageResponse response = ratingCoverageService.getRatingCoveragesForSegment(
                testPlayCreationHelper.getTenant().getId(), currentSegment.getName(), request);
        log.info("Rating Engine Segment {}, Account Count:{}, Contact Count: {}", ratingEngine.getSegment().getName(),
                currentSegment.getAccounts(), currentSegment.getContacts());
        log.info("Ratings Coverage: " + JsonUtils.serialize(response.getRatingModelsCoverageMap().get(ratingId1)));
        CoverageInfo coverage1 = response.getRatingModelsCoverageMap().get(ratingId1);
        assertRatingModelsCoverageResponse(ratingId1, response, request);

        // Check with Segment Criteria ACT_ATTR_PREMIUM_MARKETING_PRESCREEN = 2
        currentSegment = testPlayCreationHelper.createTargetSegment();
        response = ratingCoverageService.getRatingCoveragesForSegment(testPlayCreationHelper.getTenant().getId(),
                currentSegment.getName(), request);
        assertRatingModelsCoverageResponse(ratingId1, response, request);
        log.info("Rating Engine Segment {}, Account Count:{}, Contact Count: {}", currentSegment.getName(),
                currentSegment.getAccounts(), currentSegment.getContacts());
        log.info("Ratings Coverage: " + JsonUtils.serialize(response.getRatingModelsCoverageMap().get(ratingId1)));
        CoverageInfo coverage2 = response.getRatingModelsCoverageMap().get(ratingId1);

        // Check with Segment Criteria ACT_ATTR_PREMIUM_MARKETING_PRESCREEN < 3
        // And check whether the coverage counts matches with other 2 segment
        // coverages
        currentSegment = testPlayCreationHelper.createAggregatedSegment();
        response = ratingCoverageService.getRatingCoveragesForSegment(testPlayCreationHelper.getTenant().getId(),
                currentSegment.getName(), request);
        assertRatingModelsCoverageResponse(ratingId1, response, request);
        log.info("Rating Engine Segment {}, Account Count:{}, Contact Count: {}", currentSegment.getName(),
                currentSegment.getAccounts(), currentSegment.getContacts());
        log.info("Ratings Coverage: " + JsonUtils.serialize(response.getRatingModelsCoverageMap().get(ratingId1)));
        CoverageInfo coverageAggregated = response.getRatingModelsCoverageMap().get(ratingId1);

        assertEquals(new Long(coverage1.getAccountCount() + coverage2.getAccountCount()),
                coverageAggregated.getAccountCount());
        assertEquals(new Long(coverage1.getContactCount() + coverage2.getContactCount()),
                coverageAggregated.getContactCount());
        coverageAggregated.getBucketCoverageCounts().forEach(rbc -> {
            RatingBucketCoverage coverage1Bucket = coverage1.getCoverageForBucket(rbc.getBucket());
            RatingBucketCoverage coverage2Bucket = coverage2.getCoverageForBucket(rbc.getBucket());
            Long computedAccountCnt = (coverage1Bucket != null && coverage1Bucket.getCount() != null
                    ? coverage1Bucket.getCount()
                    : 0)
                    + (coverage2Bucket != null && coverage2Bucket.getCount() != null ? coverage2Bucket.getCount() : 0);
            Assert.assertEquals(computedAccountCnt, rbc.getCount(),
                    "Account Bucket Count doesnot match for: " + rbc.getBucket());

            Long computedContactCnt = (coverage1Bucket != null && coverage1Bucket.getContactCount() != null
                    ? coverage1Bucket.getContactCount()
                    : 0)
                    + (coverage2Bucket != null && coverage2Bucket.getContactCount() != null
                            ? coverage2Bucket.getContactCount()
                            : 0);
            Assert.assertEquals(computedContactCnt, rbc.getContactCount(),
                    "Contact Bucket Count doesnot match for: " + rbc.getBucket());
        });
    }

    @Test(groups = "deployment-app")
    public void testProductsCoverageForSegment() {
        String tenantId = testPlayCreationHelper.getTenant().getId();
        List<String> productIds = getProductIds(tenantId);

        Assert.assertNotNull(productIds);
        Assert.assertNotEquals(productIds.size(), 0);

        MetadataSegment currentSegment = ratingEngine.getSegment();
        RatingEngine crossSellRatingEngine = new RatingEngine();
        crossSellRatingEngine.setSegment(currentSegment);
        crossSellRatingEngine.setCreatedBy("CREATED_BY");
        crossSellRatingEngine.setUpdatedBy("UPDATED_BY");
        crossSellRatingEngine.setType(RatingEngineType.CROSS_SELL);

        RatingEngine createdRatingEngine = ratingEngineProxy
                .createOrUpdateRatingEngine(testPlayCreationHelper.getTenant().getId(), crossSellRatingEngine);
        Assert.assertNotNull(createdRatingEngine);
        ProductsCoverageRequest productsCoverageRequest = new ProductsCoverageRequest();
        productsCoverageRequest.setRatingEngine(createdRatingEngine);
        productsCoverageRequest.setProductIds(productIds);
        RatingEnginesCoverageResponse response = ratingCoverageService
                .getProductCoveragesForSegment(testPlayCreationHelper.getTenant().getId(), productsCoverageRequest, 6);

        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getRatingModelsCoverageMap());
        Assert.assertEquals(response.getRatingModelsCoverageMap().size(), productIds.size());

        for (Map.Entry<String, CoverageInfo> entry : response.getRatingModelsCoverageMap().entrySet()) {
            Long accountCount = entry.getValue().getAccountCount();
            Long unscoredAccountCount = entry.getValue().getUnscoredAccountCount();
            Long totalAccounts = accountCount + unscoredAccountCount;
            Assert.assertEquals(currentSegment.getAccounts(), totalAccounts);
        }
    }

    private List<String> getProductIds(String tenantId) {
        String servingTableName = dataCollectionProxy.getTableName(tenantId, BusinessEntity.Product.getServingStore());

        if (StringUtils.isBlank(servingTableName)) {
            return new ArrayList<>();
        }

        DataPage data = entityProxy.getProducts(tenantId);
        List<String> productIds;
        if (data != null && CollectionUtils.isNotEmpty(data.getData())) {
            productIds = data.getData().stream()
                    .map(product -> (String) product.get(InterfaceName.ProductId.toString())).limit(5)
                    .collect(Collectors.toList());
        } else {
            productIds = new ArrayList<>();
        }

        return productIds;
    }
}
